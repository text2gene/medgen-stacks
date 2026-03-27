#!/usr/bin/env python3
"""
load.py — parse NLM PubMed XML files and bulk-load into pubmed.article.

Each .xml.gz file contains up to 30,000 PubmedArticle elements.
We store the raw XML for each article (for metapub compatibility) plus
extract structured fields for fast indexed queries.

Usage (called by load.sh / Makefile):
    python3 load.py <xmlgz_file> [<xmlgz_file> ...]

Skips files already recorded in pubmed.load_log (idempotent).
"""

import gzip
import os
import sys
from pathlib import Path
from xml.etree import ElementTree as ET

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "python"))
import pg


# ── XML parsing ───────────────────────────────────────────────────────────────

def _text(el, xpath: str) -> str | None:
    found = el.find(xpath)
    if found is None:
        return None
    return "".join(found.itertext()).strip() or None


def _first_author(article) -> str | None:
    a = article.find(".//Author")
    if a is None:
        return None
    last = (a.findtext("LastName") or "").strip()
    init = (a.findtext("Initials") or "").strip()
    return f"{last} {init}".strip() or None


def _authors(article) -> str | None:
    parts = []
    for a in article.findall(".//Author"):
        last = (a.findtext("LastName") or "").strip()
        init = (a.findtext("Initials") or "").strip()
        if last:
            parts.append(f"{last} {init}".strip())
    return "; ".join(parts) if parts else None


def _year(article) -> int | None:
    for xpath in [".//PubDate/Year", ".//PubMedPubDate[@PubStatus='pubmed']/Year"]:
        yr = article.findtext(xpath)
        if yr and yr.isdigit():
            return int(yr)
    # MedlineDate fallback e.g. "2001 Jan-Feb"
    md = article.findtext(".//PubDate/MedlineDate")
    if md:
        parts = md.split()
        if parts[0].isdigit():
            return int(parts[0])
    return None


def _abstract(article) -> str | None:
    parts = [el.text for el in article.findall(".//AbstractText") if el.text]
    return " ".join(parts) if parts else None


def _article_ids(article) -> tuple[str | None, str | None]:
    doi = pmc = None
    for el in article.findall(".//ArticleId"):
        id_type = el.get("IdType", "")
        val = (el.text or "").strip()
        if id_type == "doi":
            doi = val or None
        elif id_type == "pmc":
            pmc = val or None
    return doi, pmc


def _pub_types(article) -> str | None:
    pts = [el.text for el in article.findall(".//PublicationType") if el.text]
    return "; ".join(pts) if pts else None


def _mesh_terms(article) -> str | None:
    terms = []
    for mh in article.findall(".//MeshHeading"):
        desc = mh.findtext("DescriptorName")
        if desc:
            terms.append(desc)
    return "; ".join(terms) if terms else None


def parse_article(article_el) -> dict | None:
    """Parse a single PubmedArticle element into a flat dict."""
    pmid_el = article_el.find(".//PMID")
    if pmid_el is None or not (pmid_el.text or "").strip().isdigit():
        return None
    pmid = int(pmid_el.text.strip())

    # Raw XML for this article element (for metapub compatibility)
    xml_str = ET.tostring(article_el, encoding="unicode")

    doi, pmc = _article_ids(article_el)

    j = article_el.find(".//Journal")
    journal     = (j.findtext("ISOAbbreviation") or "").strip() or None if j is not None else None
    journal_full = (j.findtext("Title") or "").strip() or None if j is not None else None
    issn        = (j.findtext(".//ISSN") or "").strip() or None if j is not None else None

    return {
        "pmid":         pmid,
        "xml":          xml_str,
        "title":        _text(article_el, ".//ArticleTitle"),
        "first_author": _first_author(article_el),
        "authors":      _authors(article_el),
        "journal":      journal,
        "journal_full": journal_full,
        "year":         _year(article_el),
        "month":        article_el.findtext(".//PubDate/Month"),
        "volume":       article_el.findtext(".//JournalIssue/Volume"),
        "issue":        article_el.findtext(".//JournalIssue/Issue"),
        "pages":        article_el.findtext(".//MedlinePgn"),
        "doi":          doi,
        "pmc_id":       pmc,
        "issn":         issn,
        "abstract":     _abstract(article_el),
        "pub_types":    _pub_types(article_el),
        "mesh_terms":   _mesh_terms(article_el),
    }


# ── File loading ──────────────────────────────────────────────────────────────

COLUMNS = [
    "pmid", "xml", "title", "first_author", "authors",
    "journal", "journal_full", "year", "month",
    "volume", "issue", "pages", "doi", "pmc_id", "issn",
    "abstract", "pub_types", "mesh_terms",
]

UPSERT_SQL = """
INSERT INTO pubmed.article ({cols})
VALUES ({placeholders})
ON CONFLICT (pmid) DO UPDATE SET
    xml          = EXCLUDED.xml,
    title        = EXCLUDED.title,
    first_author = EXCLUDED.first_author,
    authors      = EXCLUDED.authors,
    journal      = EXCLUDED.journal,
    journal_full = EXCLUDED.journal_full,
    year         = EXCLUDED.year,
    month        = EXCLUDED.month,
    volume       = EXCLUDED.volume,
    issue        = EXCLUDED.issue,
    pages        = EXCLUDED.pages,
    doi          = EXCLUDED.doi,
    pmc_id       = EXCLUDED.pmc_id,
    issn         = EXCLUDED.issn,
    abstract     = EXCLUDED.abstract,
    pub_types    = EXCLUDED.pub_types,
    mesh_terms   = EXCLUDED.mesh_terms,
    updated_at   = NOW()
""".format(
    cols=", ".join(COLUMNS),
    placeholders=", ".join(["%s"] * len(COLUMNS)),
)


def load_file(conn, path: Path) -> int:
    """Parse and load one .xml.gz file. Returns record count."""
    filename = path.name

    # Check load_log — skip if already loaded
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pubmed.load_log WHERE filename = %s", (filename,))
        if cur.fetchone():
            print(f"  Skipping (already loaded): {filename}", flush=True)
            return 0

    print(f"  Loading: {filename}", flush=True)
    opener = gzip.open if filename.endswith(".gz") else open
    records = 0

    with opener(path, "rb") as f:
        # Stream-parse — don't load entire file into memory
        context = ET.iterparse(f, events=("end",))
        batch: list[tuple] = []

        for event, elem in context:
            if elem.tag != "PubmedArticle":
                continue
            row = parse_article(elem)
            if row:
                batch.append(tuple(row.get(c) for c in COLUMNS))
                records += 1
            elem.clear()  # free memory

            if len(batch) >= 500:
                with conn.cursor() as cur:
                    cur.executemany(UPSERT_SQL, batch)
                conn.commit()
                pg.progress(records, None, label=f"  {filename}")
                batch.clear()

        if batch:
            with conn.cursor() as cur:
                cur.executemany(UPSERT_SQL, batch)
            conn.commit()

    # Record in load_log
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pubmed.load_log (filename, records_loaded) VALUES (%s, %s)"
            " ON CONFLICT (filename) DO UPDATE SET records_loaded = EXCLUDED.records_loaded, loaded_at = NOW()",
            (filename, records),
        )
    conn.commit()
    print(f"\n  Loaded {records:,} articles from {filename}", flush=True)
    return records


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <file.xml.gz> [<file.xml.gz> ...]", file=sys.stderr)
        sys.exit(1)

    conn = pg.connect()
    total = 0
    for arg in sys.argv[1:]:
        path = Path(arg)
        if not path.exists():
            print(f"  File not found: {path}", file=sys.stderr)
            continue
        total += load_file(conn, path)

    conn.close()
    print(f"\nDone. {total:,} total articles loaded.")


if __name__ == "__main__":
    main()
