#!/usr/bin/env python3
"""
load.py — parse PMC OA JATS XML bundles and load into pmc.article.

Each .tar.gz bundle contains hundreds of individual PMC*.xml files in JATS
format.  We extract title, abstract, and body text (tags stripped) plus
cross-reference fields (pmid, doi, journal, year).

Usage (called by Makefile):
    python3 load.py <bundle.tar.gz> [<bundle.tar.gz> ...]

Skips bundles already in pmc.load_log (idempotent).
"""

import re
import sys
import tarfile
from pathlib import Path
from xml.etree import ElementTree as ET

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "python"))
import pg


# ── JATS parsing helpers ──────────────────────────────────────────────────────

def _text_content(el) -> str:
    """Recursively extract all text from an element, stripping tags."""
    return " ".join((el.itertext())).strip() if el is not None else ""


def _inner_text(root, xpath) -> str | None:
    el = root.find(xpath)
    return _text_content(el) or None


def _article_id(root, id_type: str) -> str | None:
    for el in root.findall(".//article-id"):
        if el.get("pub-id-type") == id_type:
            return (el.text or "").strip() or None
    return None


def _abstract(root) -> str | None:
    parts = []
    for el in root.findall(".//abstract"):
        txt = _text_content(el)
        if txt:
            parts.append(txt)
    return " ".join(parts) or None


def _body(root) -> str | None:
    body_el = root.find(".//body")
    if body_el is None:
        return None
    # Strip tags but preserve whitespace between elements
    text = " ".join(body_el.itertext())
    # Collapse runs of whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _journal(root) -> str | None:
    for xpath in [
        ".//journal-meta/journal-id[@journal-id-type='nlm-ta']",
        ".//journal-meta/journal-id[@journal-id-type='iso-abbrev']",
        ".//journal-meta/journal-title",
    ]:
        val = _inner_text(root, xpath)
        if val:
            return val
    return None


def _year(root) -> int | None:
    for xpath in [
        ".//pub-date[@pub-type='epub']/year",
        ".//pub-date[@pub-type='ppub']/year",
        ".//pub-date/year",
    ]:
        el = root.find(xpath)
        if el is not None and (el.text or "").strip().isdigit():
            return int(el.text.strip())
    return None


def parse_jats(xml_bytes: bytes, license_type: str) -> dict | None:
    """Parse a single JATS XML article. Returns dict or None on failure."""
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return None

    pmcid = _article_id(root, "pmc")
    if not pmcid:
        # Try filename-based fallback — caller can pass it as pmcid hint
        return None

    pmid_str = _article_id(root, "pmid")
    pmid = int(pmid_str) if pmid_str and pmid_str.isdigit() else None

    title_el = root.find(".//title-group/article-title")
    title = _text_content(title_el) or None

    return {
        "pmcid":   pmcid,
        "pmid":    pmid,
        "doi":     _article_id(root, "doi"),
        "title":   title,
        "abstract": _abstract(root),
        "body":    _body(root),
        "journal": _journal(root),
        "year":    _year(root),
        "license": license_type,
    }


# ── DB loading ────────────────────────────────────────────────────────────────

COLUMNS = ["pmcid", "pmid", "doi", "title", "abstract", "body",
           "journal", "year", "license"]

UPSERT_SQL = """
INSERT INTO pmc.article ({cols})
VALUES ({placeholders})
ON CONFLICT (pmcid) DO UPDATE SET
    pmid     = EXCLUDED.pmid,
    doi      = EXCLUDED.doi,
    title    = EXCLUDED.title,
    abstract = EXCLUDED.abstract,
    body     = EXCLUDED.body,
    journal  = EXCLUDED.journal,
    year     = EXCLUDED.year,
    license  = EXCLUDED.license,
    updated_at = NOW()
""".format(
    cols=", ".join(COLUMNS),
    placeholders=", ".join(["%s"] * len(COLUMNS)),
)


def load_bundle(conn, path: Path) -> int:
    """Load one .tar.gz bundle. Returns article count."""
    filename = path.name

    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pmc.load_log WHERE filename = %s", (filename,))
        if cur.fetchone():
            print(f"  Skipping (already loaded): {filename}", flush=True)
            return 0

    # Infer license from filename/path
    license_type = "oa_comm" if "oa_comm" in str(path) else "oa_noncomm"

    print(f"  Loading: {filename}", flush=True)
    records = 0
    batch: list[tuple] = []

    with tarfile.open(path, "r:gz") as tar:
        for member in tar.getmembers():
            if not member.name.endswith(".xml"):
                continue
            f = tar.extractfile(member)
            if f is None:
                continue
            xml_bytes = f.read()
            row = parse_jats(xml_bytes, license_type)
            if not row:
                # Try to get pmcid from filename
                stem = Path(member.name).stem  # e.g. "PMC176545"
                if stem.startswith("PMC"):
                    # re-parse with pmcid hint — skip if still fails
                    pass
                continue

            batch.append(tuple(row.get(c) for c in COLUMNS))
            records += 1

            if len(batch) >= 200:
                with conn.cursor() as cur:
                    cur.executemany(UPSERT_SQL, batch)
                conn.commit()
                pg.progress(records, None, label=f"  {filename}")
                batch.clear()

    if batch:
        with conn.cursor() as cur:
            cur.executemany(UPSERT_SQL, batch)
        conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pmc.load_log (filename, records_loaded) VALUES (%s, %s)"
            " ON CONFLICT (filename) DO UPDATE SET records_loaded = EXCLUDED.records_loaded,"
            " loaded_at = NOW()",
            (filename, records),
        )
    conn.commit()
    print(f"\n  Loaded {records:,} articles from {filename}", flush=True)
    return records


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <bundle.tar.gz> [...]", file=sys.stderr)
        sys.exit(1)

    conn = pg.connect()
    total = 0
    for arg in sys.argv[1:]:
        path = Path(arg)
        if not path.exists():
            print(f"  File not found: {path}", file=sys.stderr)
            continue
        total += load_bundle(conn, path)

    conn.close()
    print(f"\nDone. {total:,} total articles loaded.")


if __name__ == "__main__":
    main()
