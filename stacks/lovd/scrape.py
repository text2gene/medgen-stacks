#!/usr/bin/env python3
"""
scrape.py — politely scrape grenada.lumc.nl/LSDB_list/lsdbs and load
gene→LOVD-instance mappings into lovd.lsdb.

Run:
    python3 scrape.py                      # all HGNC genes
    python3 scrape.py CFTR BRCA1 DMD      # specific genes only
    python3 scrape.py --refresh            # re-scrape already-seen genes

Be kind:
  - 2-second sleep between gene page requests
  - Descriptive User-Agent with contact
  - Skips genes already in scrape_log (resumable)
"""

import re
import sys
import time
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "python"))
import pg

LSDB_BASE = "https://grenada.lumc.nl/LSDB_list/lsdbs"
UA = "text2gene2/0.3 (https://text2gene.org; building LSDB index; contact: naomi@nthmost.com)"
SLEEP = 2.0


# ── HTML parsing ──────────────────────────────────────────────────────────────

def _get(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    return urllib.request.urlopen(req, timeout=30).read().decode("utf-8", errors="replace")


def parse_lsdb_page(html: str, gene: str) -> list[dict]:
    rows = re.findall(
        r'<TR[^>]*class="data[^"]*"[^>]*>(.*?)</TR>',
        html, re.DOTALL | re.IGNORECASE
    )
    instances = []
    for row in rows:
        cells = re.findall(r'<TD[^>]*>(.*?)</TD>', row, re.DOTALL | re.IGNORECASE)
        cells = [re.sub(r'<[^>]+>', ' ', c).strip() for c in cells]
        cells = [re.sub(r'\s+', ' ', c) for c in cells]
        if len(cells) < 4:
            continue

        # Cell 0: "Database name  URL" (separated by whitespace after tag stripping)
        # Find the URL in the raw cell HTML
        url_m = re.search(r'href="(https?://[^"]+)"', row, re.IGNORECASE)
        if not url_m:
            continue
        url = url_m.group(1).strip()

        # Name is first non-empty text chunk in cell 0
        name = cells[0].split("  ")[0].strip() or cells[0].strip()

        # Cell 1: curator / institution
        curator = cells[1].split("  ")[0].strip() if cells[1] else None

        # Cell 3: unique variant count
        try:
            n_variants = int(cells[3].replace(",", "").strip())
        except (ValueError, IndexError):
            n_variants = None

        # Cell 4: software type
        db_type = cells[4].strip() if len(cells) > 4 else None

        instances.append({
            "gene":       gene,
            "name":       name,
            "url":        url,
            "curator":    curator or None,
            "n_variants": n_variants,
            "db_type":    db_type or None,
        })
    return instances


# ── DB loading ────────────────────────────────────────────────────────────────

UPSERT_LSDB = """
INSERT INTO lovd.lsdb (gene, name, url, curator, n_variants, db_type, last_seen)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (gene, url) DO UPDATE SET
    name       = EXCLUDED.name,
    curator    = EXCLUDED.curator,
    n_variants = EXCLUDED.n_variants,
    db_type    = EXCLUDED.db_type,
    last_seen  = EXCLUDED.last_seen
"""

LOG_GENE = """
INSERT INTO lovd.scrape_log (gene, scraped_at, n_instances)
VALUES (%s, NOW(), %s)
ON CONFLICT (gene) DO UPDATE SET scraped_at = NOW(), n_instances = EXCLUDED.n_instances
"""


def load_gene(conn, gene: str, instances: list[dict]) -> None:
    today = date.today()
    with conn.cursor() as cur:
        for inst in instances:
            cur.execute(UPSERT_LSDB, (
                inst["gene"], inst["name"], inst["url"],
                inst["curator"], inst["n_variants"], inst["db_type"], today,
            ))
        cur.execute(LOG_GENE, (gene, len(instances)))
    conn.commit()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    refresh = "--refresh" in sys.argv
    specific = [a for a in sys.argv[1:] if not a.startswith("--")]

    conn = pg.connect()

    if specific:
        genes = specific
        print(f"Scraping {len(genes)} specified genes")
    else:
        # Full HGNC gene list from local DB
        with conn.cursor() as cur:
            cur.execute("SELECT symbol FROM hgnc.gene WHERE symbol IS NOT NULL ORDER BY symbol")
            genes = [r[0] for r in cur.fetchall()]
        print(f"Got {len(genes)} genes from HGNC")

    # Already-scraped genes (skip unless --refresh)
    done = set()
    if not refresh:
        with conn.cursor() as cur:
            cur.execute("SELECT gene FROM lovd.scrape_log")
            done = {r[0] for r in cur.fetchall()}
        print(f"Skipping {len(done)} already-scraped genes")

    todo = [g for g in genes if g not in done]
    print(f"Scraping {len(todo)} genes — estimated {len(todo) * SLEEP / 3600:.1f}h at {SLEEP}s/gene")

    for i, gene in enumerate(todo):
        try:
            html = _get(f"{LSDB_BASE}/{gene}")
            instances = parse_lsdb_page(html, gene)
            load_gene(conn, gene, instances)
        except urllib.error.HTTPError as e:
            print(f"  [{i+1}/{len(todo)}] {gene}: HTTP {e.code}", flush=True)
            load_gene(conn, gene, [])  # log as scraped even on 404
        except Exception as e:
            print(f"  [{i+1}/{len(todo)}] {gene}: {e}", flush=True)

        if i % 100 == 0:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM lovd.lsdb")
                total = cur.fetchone()[0]
            print(f"  [{i+1}/{len(todo)}] {gene} — {total:,} instances in DB", flush=True)

        time.sleep(SLEEP)

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
