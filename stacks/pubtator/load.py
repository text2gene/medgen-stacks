#!/usr/bin/env python3
"""
load.py — load PubTator Central annotation files into PostgreSQL.

Input files are gzipped TSV with NO header line.
Columns: PMID | Type | ConceptID | Mentions | Resource

For gene2pubtatorcentral.gz, ConceptID is the NCBI Gene ID (integer).
For mutation2pubtatorcentral.gz, ConceptID is an rs number or HGVS-like identifier.

Usage:
    python3 load.py genes     <gene2pubtatorcentral.gz>
    python3 load.py mutations <mutation2pubtatorcentral.gz>
"""

import gzip
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "python"))
import pg

BATCH_SIZE = 5000


def _open(path: Path):
    return gzip.open(path, "rt", encoding="utf-8", errors="replace") \
        if str(path).endswith(".gz") else open(path, encoding="utf-8", errors="replace")


def _int_or_none(val: str) -> int | None:
    val = val.strip()
    if not val:
        return None
    try:
        return int(val)
    except ValueError:
        return None


def _str_or_none(val: str) -> str | None:
    val = val.strip()
    return None if not val else val


# ── pubtator.gene_mention ─────────────────────────────────────────────────────

GM_UPSERT = """
INSERT INTO pubtator.gene_mention (pmid, gene_id, mentions, resource)
VALUES (%s, %s, %s, %s)
ON CONFLICT (pmid, gene_id) DO UPDATE SET
    mentions = EXCLUDED.mentions,
    resource = EXCLUDED.resource
"""


def load_genes(conn, path: Path) -> int:
    filename = path.name
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pubtator.load_log WHERE filename = %s", (filename,))
        if cur.fetchone():
            print(f"  Skipping (already loaded): {filename}")
            return 0

    print(f"  Loading gene_mention: {path}", flush=True)
    count = 0
    skipped = 0
    batch = []

    # Format (no header): PMID \t Type \t ConceptID \t Mentions \t Resource
    with _open(path) as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 5:
                skipped += 1
                continue

            pmid    = _int_or_none(parts[0])
            gene_id = _int_or_none(parts[2])

            if pmid is None or gene_id is None:
                skipped += 1
                continue

            mentions = _str_or_none(parts[3])
            resource = _str_or_none(parts[4])

            batch.append((pmid, gene_id, mentions, resource))
            count += 1

            if len(batch) >= BATCH_SIZE:
                with conn.cursor() as cur:
                    cur.executemany(GM_UPSERT, batch)
                conn.commit()
                pg.progress(count, None, "  gene_mention")
                batch.clear()

    if batch:
        with conn.cursor() as cur:
            cur.executemany(GM_UPSERT, batch)
        conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pubtator.load_log (filename, records_loaded) VALUES (%s, %s)"
            " ON CONFLICT (filename) DO UPDATE SET records_loaded = EXCLUDED.records_loaded, loaded_at = NOW()",
            (filename, count),
        )
    conn.commit()
    print(f"\n  Loaded {count:,} gene mentions from {filename} (skipped {skipped:,} malformed rows)")
    return count


# ── pubtator.mutation_mention ─────────────────────────────────────────────────

MM_UPSERT = """
INSERT INTO pubtator.mutation_mention (pmid, concept_id, mentions, resource)
VALUES (%s, %s, %s, %s)
ON CONFLICT (pmid, concept_id) DO UPDATE SET
    mentions = EXCLUDED.mentions,
    resource = EXCLUDED.resource
"""


def load_mutations(conn, path: Path) -> int:
    filename = path.name
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pubtator.load_log WHERE filename = %s", (filename,))
        if cur.fetchone():
            print(f"  Skipping (already loaded): {filename}")
            return 0

    print(f"  Loading mutation_mention: {path}", flush=True)
    count = 0
    skipped = 0
    batch = []

    # Format (no header): PMID \t Type \t ConceptID \t Mentions \t Resource
    with _open(path) as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 5:
                skipped += 1
                continue

            pmid       = _int_or_none(parts[0])
            concept_id = _str_or_none(parts[2])

            if pmid is None or concept_id is None:
                skipped += 1
                continue

            mentions = _str_or_none(parts[3])
            resource = _str_or_none(parts[4])

            batch.append((pmid, concept_id, mentions, resource))
            count += 1

            if len(batch) >= BATCH_SIZE:
                with conn.cursor() as cur:
                    cur.executemany(MM_UPSERT, batch)
                conn.commit()
                pg.progress(count, None, "  mutation_mention")
                batch.clear()

    if batch:
        with conn.cursor() as cur:
            cur.executemany(MM_UPSERT, batch)
        conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pubtator.load_log (filename, records_loaded) VALUES (%s, %s)"
            " ON CONFLICT (filename) DO UPDATE SET records_loaded = EXCLUDED.records_loaded, loaded_at = NOW()",
            (filename, count),
        )
    conn.commit()
    print(f"\n  Loaded {count:,} mutation mentions from {filename} (skipped {skipped:,} malformed rows)")
    return count


# ── Entry point ───────────────────────────────────────────────────────────────

LOADERS = {
    "genes":     load_genes,
    "mutations": load_mutations,
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
