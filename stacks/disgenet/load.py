#!/usr/bin/env python3
"""
load.py — load DisGeNET curated association files into PostgreSQL.

Input files are gzipped TSV with a header line.
score is a float in [0, 1].
pmids may appear as semicolon-separated values in the source PMID column.

Usage:
    python3 load.py gene_disease    <curated_gene_disease_associations.tsv.gz>
    python3 load.py variant_disease <curated_variant_disease_associations.tsv.gz>
"""

import csv
import gzip
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "python"))
import pg


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


def _float_or_none(val: str) -> float | None:
    val = val.strip()
    if val in ("", "-"):
        return None
    try:
        return float(val)
    except ValueError:
        return None


# ── disgenet.gene_disease ─────────────────────────────────────────────────────

GD_UPSERT = """
INSERT INTO disgenet.gene_disease (
    gene_id, gene_symbol, disease_id, disease_name, disease_type, score, pmids, source
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (gene_id, disease_id) DO UPDATE SET
    gene_symbol  = EXCLUDED.gene_symbol,
    disease_name = EXCLUDED.disease_name,
    disease_type = EXCLUDED.disease_type,
    score        = EXCLUDED.score,
    pmids        = EXCLUDED.pmids,
    source       = EXCLUDED.source
"""


def load_gene_disease(conn, path: Path) -> int:
    filename = path.name
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM disgenet.load_log WHERE filename = %s", (filename,))
        if cur.fetchone():
            print(f"  Skipping (already loaded): {filename}")
            return 0

    print(f"  Loading gene_disease: {path}", flush=True)
    count = 0
    batch = []

    # TSV columns (header present):
    # geneId, geneSymbol, DSI, DPI, diseaseId, diseaseName, diseaseType,
    # diseaseClass, diseaseSemanticType, score, EI, YI, YF, PMID, source
    with _open(path) as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            gene_id      = _int_or_none(row.get("geneId", ""))
            disease_id   = _str_or_none(row.get("diseaseId", ""))

            if gene_id is None or disease_id is None:
                continue

            batch.append((
                gene_id,
                _str_or_none(row.get("geneSymbol", "")),
                disease_id,
                _str_or_none(row.get("diseaseName", "")),
                _str_or_none(row.get("diseaseType", "")),
                _float_or_none(row.get("score", "")),
                _str_or_none(row.get("PMID", "")),
                _str_or_none(row.get("source", "")),
            ))
            count += 1

            if len(batch) >= 1000:
                with conn.cursor() as cur:
                    cur.executemany(GD_UPSERT, batch)
                conn.commit()
                pg.progress(count, None, "  gene_disease")
                batch.clear()

    if batch:
        with conn.cursor() as cur:
            cur.executemany(GD_UPSERT, batch)
        conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO disgenet.load_log (filename, records_loaded) VALUES (%s, %s)"
            " ON CONFLICT (filename) DO UPDATE SET records_loaded = EXCLUDED.records_loaded, loaded_at = NOW()",
            (filename, count),
        )
    conn.commit()
    print(f"\n  Loaded {count:,} gene-disease associations from {filename}")
    return count


# ── disgenet.variant_disease ──────────────────────────────────────────────────

VD_UPSERT = """
INSERT INTO disgenet.variant_disease (
    snp_id, chromosome, position, disease_id, disease_name, score, pmids, source
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (snp_id, disease_id) DO UPDATE SET
    chromosome   = EXCLUDED.chromosome,
    position     = EXCLUDED.position,
    disease_name = EXCLUDED.disease_name,
    score        = EXCLUDED.score,
    pmids        = EXCLUDED.pmids,
    source       = EXCLUDED.source
"""


def load_variant_disease(conn, path: Path) -> int:
    filename = path.name
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM disgenet.load_log WHERE filename = %s", (filename,))
        if cur.fetchone():
            print(f"  Skipping (already loaded): {filename}")
            return 0

    print(f"  Loading variant_disease: {path}", flush=True)
    count = 0
    batch = []

    # TSV columns (header present):
    # snpId, chromosome, position, DSI, DPI, diseaseId, diseaseName, diseaseType,
    # diseaseClass, diseaseSemanticType, score, EI, YI, YF, PMID, source
    with _open(path) as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            snp_id     = _str_or_none(row.get("snpId", ""))
            disease_id = _str_or_none(row.get("diseaseId", ""))

            if snp_id is None or disease_id is None:
                continue

            batch.append((
                snp_id,
                _str_or_none(row.get("chromosome", "")),
                _int_or_none(row.get("position", "")),
                disease_id,
                _str_or_none(row.get("diseaseName", "")),
                _float_or_none(row.get("score", "")),
                _str_or_none(row.get("PMID", "")),
                _str_or_none(row.get("source", "")),
            ))
            count += 1

            if len(batch) >= 1000:
                with conn.cursor() as cur:
                    cur.executemany(VD_UPSERT, batch)
                conn.commit()
                pg.progress(count, None, "  variant_disease")
                batch.clear()

    if batch:
        with conn.cursor() as cur:
            cur.executemany(VD_UPSERT, batch)
        conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO disgenet.load_log (filename, records_loaded) VALUES (%s, %s)"
            " ON CONFLICT (filename) DO UPDATE SET records_loaded = EXCLUDED.records_loaded, loaded_at = NOW()",
            (filename, count),
        )
    conn.commit()
    print(f"\n  Loaded {count:,} variant-disease associations from {filename}")
    return count


# ── Entry point ───────────────────────────────────────────────────────────────

LOADERS = {
    "gene_disease":    load_gene_disease,
    "variant_disease": load_variant_disease,
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
