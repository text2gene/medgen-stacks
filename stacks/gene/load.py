#!/usr/bin/env python3
"""
load.py — load NCBI Entrez Gene files into PostgreSQL.

Filters for human (tax_id = 9606) only.
Input files are gzipped TSV with a header line starting with '#'.

Usage:
    python3 load.py info    <gene_info.gz>
    python3 load.py pubmed  <gene2pubmed.gz>
    python3 load.py history <gene_history.gz>
"""

import csv
import gzip
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "python"))
import pg

HUMAN_TAX_ID = "9606"


def _open(path: Path):
    return gzip.open(path, "rt", encoding="utf-8", errors="replace") \
        if str(path).endswith(".gz") else open(path, encoding="utf-8", errors="replace")


def _str_or_none(val: str) -> str | None:
    val = val.strip()
    return None if val in ("", "-") else val


def _int_or_none(val: str) -> int | None:
    val = val.strip()
    if val in ("", "-"):
        return None
    try:
        return int(val)
    except ValueError:
        return None


def _strip_hash_header(f):
    """
    Wrap a file object so that the first line has its leading '#' stripped,
    making it a valid header for csv.DictReader.
    """
    first = True
    for line in f:
        if first:
            first = False
            yield line.lstrip("#")
        else:
            yield line


# ── gene.info ────────────────────────────────────────────────────────────────

INFO_UPSERT = """
INSERT INTO gene.info (
    gene_id, tax_id, symbol, synonyms, description,
    type_of_gene, full_name, chromosome, map_location
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (gene_id) DO UPDATE SET
    tax_id       = EXCLUDED.tax_id,
    symbol       = EXCLUDED.symbol,
    synonyms     = EXCLUDED.synonyms,
    description  = EXCLUDED.description,
    type_of_gene = EXCLUDED.type_of_gene,
    full_name    = EXCLUDED.full_name,
    chromosome   = EXCLUDED.chromosome,
    map_location = EXCLUDED.map_location,
    updated_at   = NOW()
"""


def load_info(conn, path: Path) -> int:
    filename = path.name
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM gene.load_log WHERE filename = %s", (filename,))
        if cur.fetchone():
            print(f"  Skipping (already loaded): {filename}")
            return 0

    print(f"  Loading gene.info: {path}", flush=True)
    count = 0
    batch = []

    with _open(path) as f:
        reader = csv.DictReader(_strip_hash_header(f), delimiter="\t")
        for row in reader:
            if row.get("tax_id", "").strip() != HUMAN_TAX_ID:
                continue

            batch.append((
                _int_or_none(row.get("GeneID", "")),
                _int_or_none(row.get("tax_id", "")),
                _str_or_none(row.get("Symbol", "")),
                _str_or_none(row.get("Synonyms", "")),
                _str_or_none(row.get("description", "")),
                _str_or_none(row.get("type_of_gene", "")),
                _str_or_none(row.get("Full_name_from_nomenclature_authority", "")),
                _str_or_none(row.get("chromosome", "")),
                _str_or_none(row.get("map_location", "")),
            ))
            count += 1

            if len(batch) >= 1000:
                with conn.cursor() as cur:
                    cur.executemany(INFO_UPSERT, batch)
                conn.commit()
                pg.progress(count, None, "  gene.info")
                batch.clear()

    if batch:
        with conn.cursor() as cur:
            cur.executemany(INFO_UPSERT, batch)
        conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO gene.load_log (filename, records_loaded) VALUES (%s, %s)"
            " ON CONFLICT (filename) DO UPDATE SET records_loaded = EXCLUDED.records_loaded, loaded_at = NOW()",
            (filename, count),
        )
    conn.commit()
    print(f"\n  Loaded {count:,} gene records from {filename}")
    return count


# ── gene.pubmed ───────────────────────────────────────────────────────────────

PUBMED_UPSERT = """
INSERT INTO gene.pubmed (gene_id, pmid, tax_id)
VALUES (%s, %s, %s)
ON CONFLICT DO NOTHING
"""


def load_pubmed(conn, path: Path) -> int:
    filename = path.name
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM gene.load_log WHERE filename = %s", (filename,))
        if cur.fetchone():
            print(f"  Skipping (already loaded): {filename}")
            return 0

    print(f"  Loading gene.pubmed: {path}", flush=True)
    count = 0
    batch = []

    with _open(path) as f:
        reader = csv.DictReader(_strip_hash_header(f), delimiter="\t")
        for row in reader:
            if row.get("tax_id", "").strip() != HUMAN_TAX_ID:
                continue

            gene_id = _int_or_none(row.get("GeneID", ""))
            pmid    = _int_or_none(row.get("PubMed_ID", ""))
            tax_id  = _int_or_none(row.get("tax_id", ""))

            if gene_id is None or pmid is None:
                continue

            batch.append((gene_id, pmid, tax_id))
            count += 1

            if len(batch) >= 2000:
                with conn.cursor() as cur:
                    cur.executemany(PUBMED_UPSERT, batch)
                conn.commit()
                pg.progress(count, None, "  gene.pubmed")
                batch.clear()

    if batch:
        with conn.cursor() as cur:
            cur.executemany(PUBMED_UPSERT, batch)
        conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO gene.load_log (filename, records_loaded) VALUES (%s, %s)"
            " ON CONFLICT (filename) DO UPDATE SET records_loaded = EXCLUDED.records_loaded, loaded_at = NOW()",
            (filename, count),
        )
    conn.commit()
    print(f"\n  Loaded {count:,} gene→pubmed associations from {filename}")
    return count


# ── gene.history ──────────────────────────────────────────────────────────────

HISTORY_UPSERT = """
INSERT INTO gene.history (tax_id, discontinued_gene_id, discontinued_symbol, current_gene_id)
VALUES (%s, %s, %s, %s)
ON CONFLICT (discontinued_gene_id) DO UPDATE SET
    tax_id             = EXCLUDED.tax_id,
    discontinued_symbol = EXCLUDED.discontinued_symbol,
    current_gene_id    = EXCLUDED.current_gene_id
"""


def load_history(conn, path: Path) -> int:
    filename = path.name
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM gene.load_log WHERE filename = %s", (filename,))
        if cur.fetchone():
            print(f"  Skipping (already loaded): {filename}")
            return 0

    print(f"  Loading gene.history: {path}", flush=True)
    count = 0
    batch = []

    with _open(path) as f:
        reader = csv.DictReader(_strip_hash_header(f), delimiter="\t")
        for row in reader:
            if row.get("tax_id", "").strip() != HUMAN_TAX_ID:
                continue

            tax_id               = _int_or_none(row.get("tax_id", ""))
            discontinued_gene_id = _int_or_none(row.get("discontinued_GeneID", ""))
            discontinued_symbol  = _str_or_none(row.get("discontinued_Symbol", ""))
            current_gene_id      = _int_or_none(row.get("GeneID", ""))

            if discontinued_gene_id is None:
                continue

            batch.append((tax_id, discontinued_gene_id, discontinued_symbol, current_gene_id))
            count += 1

            if len(batch) >= 2000:
                with conn.cursor() as cur:
                    cur.executemany(HISTORY_UPSERT, batch)
                conn.commit()
                pg.progress(count, None, "  gene.history")
                batch.clear()

    if batch:
        with conn.cursor() as cur:
            cur.executemany(HISTORY_UPSERT, batch)
        conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO gene.load_log (filename, records_loaded) VALUES (%s, %s)"
            " ON CONFLICT (filename) DO UPDATE SET records_loaded = EXCLUDED.records_loaded, loaded_at = NOW()",
            (filename, count),
        )
    conn.commit()
    print(f"\n  Loaded {count:,} history records from {filename}")
    return count


# ── Entry point ───────────────────────────────────────────────────────────────

LOADERS = {
    "info":    load_info,
    "pubmed":  load_pubmed,
    "history": load_history,
}


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <table> <file>")
        print(f"Tables: {', '.join(LOADERS)}")
        sys.exit(1)

    table, path = sys.argv[1], Path(sys.argv[2])
    if table not in LOADERS:
        print(f"Unknown table: {table}. Choose from: {', '.join(LOADERS)}")
        sys.exit(1)

    conn = pg.connect()
    LOADERS[table](conn, path)
    conn.close()


if __name__ == "__main__":
    main()
