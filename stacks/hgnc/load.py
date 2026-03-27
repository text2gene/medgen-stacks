#!/usr/bin/env python3
"""
load.py — load HGNC gene data into PostgreSQL.

Usage:
    python3 load.py genes <hgnc_complete_set.txt>
"""

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "python"))
import pg


def _str_or_none(val: str) -> str | None:
    val = val.strip()
    return val if val else None


def _int_or_none(val: str) -> int | None:
    val = val.strip()
    try:
        return int(val) if val not in ("", "-1", ".", "na", "NA") else None
    except ValueError:
        return None


# ── genes ─────────────────────────────────────────────────────────────────────

GENE_UPSERT = """
INSERT INTO hgnc.gene (
    hgnc_id, symbol, name, locus_group, locus_type, status, location,
    alias_symbols, prev_symbols, entrez_id, ensembl_gene_id,
    omim_id, uniprot_ids, refseq_accession
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (hgnc_id) DO UPDATE SET
    symbol           = EXCLUDED.symbol,
    name             = EXCLUDED.name,
    locus_group      = EXCLUDED.locus_group,
    locus_type       = EXCLUDED.locus_type,
    status           = EXCLUDED.status,
    location         = EXCLUDED.location,
    alias_symbols    = EXCLUDED.alias_symbols,
    prev_symbols     = EXCLUDED.prev_symbols,
    entrez_id        = EXCLUDED.entrez_id,
    ensembl_gene_id  = EXCLUDED.ensembl_gene_id,
    omim_id          = EXCLUDED.omim_id,
    uniprot_ids      = EXCLUDED.uniprot_ids,
    refseq_accession = EXCLUDED.refseq_accession
"""


def load_genes(conn, path: Path) -> int:
    filename = path.name
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM hgnc.load_log WHERE filename = %s", (filename,))
        if cur.fetchone():
            print(f"  Skipping (already loaded): {filename}")
            return 0

    print(f"  Loading HGNC genes: {path}", flush=True)
    count = 0
    batch = []

    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            if row.get("status", "").strip() != "Approved":
                continue

            batch.append((
                _str_or_none(row.get("hgnc_id", "")),
                _str_or_none(row.get("symbol", "")),
                _str_or_none(row.get("name", "")),
                _str_or_none(row.get("locus_group", "")),
                _str_or_none(row.get("locus_type", "")),
                _str_or_none(row.get("status", "")),
                _str_or_none(row.get("location", "")),
                _str_or_none(row.get("alias_symbol", "")),
                _str_or_none(row.get("prev_symbol", "")),
                _int_or_none(row.get("entrez_id", "")),
                _str_or_none(row.get("ensembl_gene_id", "")),
                _str_or_none(row.get("omim_id", "")),
                _str_or_none(row.get("uniprot_ids", "")),
                _str_or_none(row.get("refseq_accession", "")),
            ))
            count += 1

            if len(batch) >= 500:
                with conn.cursor() as cur:
                    cur.executemany(GENE_UPSERT, batch)
                conn.commit()
                pg.progress(count, None, "  genes")
                batch.clear()

    if batch:
        with conn.cursor() as cur:
            cur.executemany(GENE_UPSERT, batch)
        conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO hgnc.load_log (filename, records_loaded) VALUES (%s, %s)"
            " ON CONFLICT (filename) DO UPDATE SET records_loaded = EXCLUDED.records_loaded, loaded_at = NOW()",
            (filename, count),
        )
    conn.commit()
    print(f"\n  Loaded {count:,} approved genes from {filename}")
    return count


# ── Entry point ───────────────────────────────────────────────────────────────

LOADERS = {
    "genes": load_genes,
}


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <subcommand> <file>")
        print(f"Subcommands: {', '.join(LOADERS)}")
        sys.exit(1)

    subcommand, path = sys.argv[1], Path(sys.argv[2])
    if subcommand not in LOADERS:
        print(f"Unknown subcommand: {subcommand}. Choose from: {', '.join(LOADERS)}")
        sys.exit(1)

    conn = pg.connect()
    LOADERS[subcommand](conn, path)
    conn.close()


if __name__ == "__main__":
    main()
