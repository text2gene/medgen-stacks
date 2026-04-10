#!/usr/bin/env python3
"""
harvest.py — politely harvest variant+reference data from LOVD instances.

Reads gene→instance mappings from lovd.lsdb, fetches variant view pages,
parses out ALL available data per variant row (HGVS, protein, exon, dbSNP,
references, curator, classification, technique, etc.) and loads into lovd tables.

Strategy for politeness:
  - Fan out across multiple LOVD hosts, round-robin style
  - Never hit the same host more than once per SLEEP seconds
  - Descriptive User-Agent with contact info
  - Skip databases.lovd.nl (we're blocked — see issue #23)
  - Resumable: skips genes already in harvest_log

Run:
    python3 harvest.py                     # all non-blocked LOVD instances
    python3 harvest.py CFTR GLA PAH       # specific genes only
    python3 harvest.py --host cftr.lovd.parseq.pro  # specific host only
    python3 harvest.py --include-blocked   # include databases.lovd.nl (if unblocked)
    python3 harvest.py --refresh           # re-harvest already-done genes
"""

import json
import re
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict, deque
from datetime import date
from html import unescape
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "python"))
import pg

UA = "text2gene2/0.4 (https://text2gene.org; harvesting variant references; contact: naomi@nthmost.com)"
SLEEP_PER_HOST = 5.0  # seconds between requests to the same host
BLOCKED_HOSTS = {"databases.lovd.nl"}

re_pubmed = re.compile(r"pubmed/(\d+)")
re_doi = re.compile(r'(10[.][0-9]{2,}(?:[.][0-9]+)*/(?:(?!["&\'\\])\S)+)')
re_data_row = re.compile(
    r'<TR[^>]*class="data[^"]*"[^>]*>(.*?)</TR>', re.DOTALL | re.IGNORECASE
)
re_transcript = re.compile(r"using the (NM_\d+\.\d+) transcript")


def _get(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    return urllib.request.urlopen(req, timeout=30).read().decode("utf-8", errors="replace")


def _host_from_url(url: str) -> str:
    return url.split("://")[1].split("/")[0] if "://" in url else url


def _base_from_gene_url(gene_url: str) -> str:
    """Convert a genes/ URL to the base URL for view paths."""
    parts = gene_url.split("://")
    if len(parts) < 2:
        return gene_url
    scheme = parts[0]
    rest = parts[1]
    host = rest.split("/")[0]
    genes_idx = rest.find("/genes/")
    prefix = rest[len(host):genes_idx] if genes_idx > 0 else ""
    return f"{scheme}://{host}{prefix}"


def _strip_html(s: str) -> str:
    """Remove HTML tags from a string."""
    return unescape(re.sub(r"<[^>]+>", " ", s)).strip()


def _extract_cell_text(td_html: str) -> str:
    """Extract clean text from a TD element."""
    return re.sub(r"\s+", " ", _strip_html(td_html)).strip()


def _extract_pmids(html_fragment: str) -> list[int]:
    return [int(p) for p in set(re_pubmed.findall(html_fragment))]


def _extract_dois(html_fragment: str) -> list[str]:
    return [d.rstrip("\\.") for d in set(re_doi.findall(html_fragment))]


def parse_variant_page(html: str, gene: str) -> tuple[list[str], list[dict]]:
    """
    Parse an LOVD variant view page into variant records with all available fields.

    Returns (column_names, variants) where each variant is a dict with:
      - All columns from the table (keyed by LOVD field name)
      - Extracted pmids and dois from reference columns
      - transcript, hgvs_cdna, hgvs_full derived fields
    """
    # Find transcript
    transcript_match = re_transcript.search(html)
    transcript = transcript_match.group(1) if transcript_match else None

    # Extract column field names from TH data-fieldname attributes
    # These appear in order matching the TD cells
    field_names = re.findall(r'data-fieldname="([^"]+)"', html)
    if not field_names:
        return [], []

    variants = []
    for row_match in re_data_row.finditer(html):
        row_html = row_match.group(1)

        # Extract all TD cells
        cells = re.findall(r"<TD[^>]*>(.*?)</TD>", row_html, re.DOTALL | re.IGNORECASE)
        if len(cells) < 3:
            continue

        record: dict = {
            "gene": gene,
            "transcript": transcript,
            "pmids": [],
            "dois": [],
        }

        # Map cells to field names
        for i, cell_html in enumerate(cells):
            if i >= len(field_names):
                break
            fname = field_names[i]
            text = _extract_cell_text(cell_html)

            # Store the clean text value
            record[fname] = text if text else None

            # Extract references from any column that contains them
            pmids = _extract_pmids(cell_html)
            if pmids:
                record["pmids"].extend(pmids)
            dois = _extract_dois(cell_html)
            if dois:
                record["dois"].extend(dois)

        # Deduplicate refs
        record["pmids"] = list(set(record["pmids"]))
        record["dois"] = list(set(record["dois"]))

        # Derive standard fields
        cdna = record.get("VariantOnTranscript/DNA") or record.get("VariantOnGenome/DNA")
        if cdna:
            record["hgvs_cdna"] = cdna
            record["hgvs_full"] = f"{transcript}:{cdna}" if transcript else cdna
        else:
            record["hgvs_cdna"] = None
            record["hgvs_full"] = None

        record["hgvs_protein"] = record.get("VariantOnTranscript/Protein")
        record["dbsnp"] = record.get("VariantOnGenome/dbSNP")
        record["lovd_dbid"] = record.get("VariantOnGenome/DBID")
        record["exon"] = record.get("VariantOnTranscript/Exon")
        record["effect"] = record.get("vot_effect")
        record["owner"] = record.get("owned_by_")

        if record["hgvs_cdna"]:
            variants.append(record)

    return field_names, variants


# ── DB operations ──────────────────────────────────────────────────────────

CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS lovd.variant (
    gene          TEXT NOT NULL,
    hgvs_cdna     TEXT NOT NULL,
    hgvs_full     TEXT,
    hgvs_protein  TEXT,
    transcript    TEXT,
    exon          TEXT,
    dbsnp         TEXT,
    lovd_dbid     TEXT,
    effect        TEXT,
    owner         TEXT,
    source_host   TEXT NOT NULL,
    source_url    TEXT NOT NULL,
    all_fields    JSONB,
    harvested_at  DATE NOT NULL,
    UNIQUE (gene, hgvs_cdna, source_host)
);

CREATE TABLE IF NOT EXISTS lovd.variant_ref (
    gene          TEXT NOT NULL,
    hgvs_cdna     TEXT NOT NULL,
    source_host   TEXT NOT NULL,
    ref_type      TEXT NOT NULL,    -- 'pmid' or 'doi'
    ref_id        TEXT NOT NULL,
    UNIQUE (gene, hgvs_cdna, source_host, ref_type, ref_id)
);

CREATE TABLE IF NOT EXISTS lovd.harvest_log (
    gene          TEXT NOT NULL,
    source_host   TEXT NOT NULL,
    harvested_at  TIMESTAMP DEFAULT NOW(),
    n_variants    INT,
    n_refs        INT,
    columns_found TEXT[],
    error         TEXT,
    UNIQUE (gene, source_host)
);

CREATE INDEX IF NOT EXISTS lovd_variant_gene_idx ON lovd.variant (gene);
CREATE INDEX IF NOT EXISTS lovd_variant_ref_gene_idx ON lovd.variant_ref (gene);
CREATE INDEX IF NOT EXISTS lovd_variant_ref_pmid_idx ON lovd.variant_ref (ref_id) WHERE ref_type = 'pmid';
"""

UPSERT_VARIANT = """
INSERT INTO lovd.variant (gene, hgvs_cdna, hgvs_full, hgvs_protein, transcript,
    exon, dbsnp, lovd_dbid, effect, owner, source_host, source_url, all_fields, harvested_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (gene, hgvs_cdna, source_host) DO UPDATE SET
    hgvs_full = EXCLUDED.hgvs_full,
    hgvs_protein = EXCLUDED.hgvs_protein,
    transcript = EXCLUDED.transcript,
    exon = EXCLUDED.exon,
    dbsnp = EXCLUDED.dbsnp,
    lovd_dbid = EXCLUDED.lovd_dbid,
    effect = EXCLUDED.effect,
    owner = EXCLUDED.owner,
    source_url = EXCLUDED.source_url,
    all_fields = EXCLUDED.all_fields,
    harvested_at = EXCLUDED.harvested_at
"""

UPSERT_REF = """
INSERT INTO lovd.variant_ref (gene, hgvs_cdna, source_host, ref_type, ref_id)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
"""

LOG_HARVEST = """
INSERT INTO lovd.harvest_log (gene, source_host, harvested_at, n_variants, n_refs, columns_found, error)
VALUES (%s, %s, NOW(), %s, %s, %s, %s)
ON CONFLICT (gene, source_host) DO UPDATE SET
    harvested_at = NOW(),
    n_variants = EXCLUDED.n_variants,
    n_refs = EXCLUDED.n_refs,
    columns_found = EXCLUDED.columns_found,
    error = EXCLUDED.error
"""


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLES)
    conn.commit()


def load_variants(conn, gene: str, host: str, url: str,
                  columns: list[str], variants: list[dict]):
    today = date.today()
    total_refs = 0
    with conn.cursor() as cur:
        for v in variants:
            # Build the all_fields JSON — exclude derived keys and large lists
            all_fields = {k: v2 for k, v2 in v.items()
                         if k not in ("gene", "transcript", "pmids", "dois",
                                      "hgvs_cdna", "hgvs_full", "hgvs_protein",
                                      "dbsnp", "lovd_dbid", "exon", "effect", "owner")
                         and v2 is not None}

            cur.execute(UPSERT_VARIANT, (
                gene, v["hgvs_cdna"], v["hgvs_full"], v.get("hgvs_protein"),
                v.get("transcript"), v.get("exon"), v.get("dbsnp"),
                v.get("lovd_dbid"), v.get("effect"), v.get("owner"),
                host, url, json.dumps(all_fields), today,
            ))
            for pmid in v["pmids"]:
                cur.execute(UPSERT_REF, (gene, v["hgvs_cdna"], host, "pmid", str(pmid)))
                total_refs += 1
            for doi in v["dois"]:
                cur.execute(UPSERT_REF, (gene, v["hgvs_cdna"], host, "doi", doi))
                total_refs += 1

        cur.execute(LOG_HARVEST, (gene, host, len(variants), total_refs, columns, None))
    conn.commit()
    return total_refs


def log_error(conn, gene: str, host: str, error: str):
    with conn.cursor() as cur:
        cur.execute(LOG_HARVEST, (gene, host, 0, 0, None, error))
    conn.commit()


# ── Main ──────────────────────────────────────────────────────────────────

def main():
    include_blocked = "--include-blocked" in sys.argv
    refresh = "--refresh" in sys.argv
    host_filter = None
    specific_genes = []

    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    for a in args:
        if "." in a and "/" not in a:
            host_filter = a
        else:
            specific_genes.append(a)

    conn = pg.connect()
    ensure_schema(conn)

    # Get all LOVD 3.X instances with variants
    with conn.cursor() as cur:
        cur.execute("""
            SELECT gene, url, n_variants
            FROM lovd.lsdb
            WHERE db_type = 'LOVD 3.X' AND n_variants > 0
            ORDER BY gene
        """)
        instances = cur.fetchall()

    # Build work queue
    work = []
    for gene, url, n_variants in instances:
        host = _host_from_url(url)
        if not include_blocked and host in BLOCKED_HOSTS:
            continue
        if host_filter and host != host_filter:
            continue
        if specific_genes and gene not in specific_genes:
            continue
        work.append((gene, url, host))

    # Filter already-harvested
    if not refresh:
        with conn.cursor() as cur:
            cur.execute("SELECT gene, source_host FROM lovd.harvest_log WHERE error IS NULL")
            done = {(r[0], r[1]) for r in cur.fetchall()}
        before = len(work)
        work = [(g, u, h) for g, u, h in work if (g, h) not in done]
        print(f"Skipping {before - len(work)} already-harvested gene/host pairs")

    # Group by host and round-robin
    by_host: dict[str, deque] = defaultdict(deque)
    for gene, url, host in work:
        by_host[host].append((gene, url))

    hosts = list(by_host.keys())
    if not hosts:
        print("Nothing to harvest.")
        conn.close()
        return

    print(f"Harvesting {len(work)} gene/host pairs across {len(hosts)} hosts")
    print(f"Estimated time: {len(work) * SLEEP_PER_HOST / max(len(hosts), 1) / 3600:.1f}h "
          f"at {SLEEP_PER_HOST}s/host with {len(hosts)}-way fan-out")

    last_request: dict[str, float] = {}
    total_variants = 0
    total_refs = 0
    errors = 0
    completed = 0

    while any(by_host[h] for h in hosts):
        for host in hosts:
            if not by_host[host]:
                continue

            now = time.monotonic()
            elapsed = now - last_request.get(host, 0)
            if elapsed < SLEEP_PER_HOST:
                time.sleep(SLEEP_PER_HOST - elapsed)

            gene, url = by_host[host].popleft()
            base = _base_from_gene_url(url)
            view_url = f"{base}/view/{gene}"

            try:
                html = _get(view_url)
                columns, variants = parse_variant_page(html, gene)
                n_refs = load_variants(conn, gene, host, url, columns, variants)
                total_variants += len(variants)
                total_refs += n_refs
                completed += 1

                if completed % 10 == 0 or len(variants) > 50:
                    print(f"  [{completed}/{len(work)}] {host}/{gene}: "
                          f"{len(variants)} variants, {n_refs} refs "
                          f"({len(columns)} columns)", flush=True)

            except urllib.error.HTTPError as e:
                print(f"  [{completed}/{len(work)}] {host}/{gene}: HTTP {e.code}", flush=True)
                log_error(conn, gene, host, f"HTTP {e.code}")
                errors += 1
            except Exception as e:
                print(f"  [{completed}/{len(work)}] {host}/{gene}: {e}", flush=True)
                log_error(conn, gene, host, str(e)[:200])
                errors += 1

            last_request[host] = time.monotonic()

    conn.close()
    print(f"\nDone. {completed} genes, {total_variants} variants, "
          f"{total_refs} references, {errors} errors.")


if __name__ == "__main__":
    main()
