#!/usr/bin/env python3
"""
load.py — load ClinVar TSV files into PostgreSQL.

Usage:
    python3 load.py variant_summary <variant_summary.txt.gz>
    python3 load.py var_citations   <var_citations.txt>
"""

import csv
import gzip
import io
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "python"))
import pg


def _open(path: Path):
    return gzip.open(path, "rt", encoding="utf-8", errors="replace") \
        if str(path).endswith(".gz") else open(path, encoding="utf-8", errors="replace")


def _int_or_none(val: str) -> int | None:
    val = val.strip()
    try:
        return int(val) if val not in ("", "-1", ".", "na", "NA") else None
    except ValueError:
        return None


def _date_or_none(val: str) -> str | None:
    val = val.strip()
    return val if val and val != "-" else None


# ── variant_summary ───────────────────────────────────────────────────────────

VS_UPSERT = """
INSERT INTO clinvar.variant_summary (
    allele_id, variation_id, name, gene_id, gene_symbol,
    hgvs_cdna, hgvs_protein, molecular_consequence,
    clinical_significance, clinsig_simple, review_status,
    last_evaluated, phenotype_ids, phenotype_list,
    chromosome, start, stop, assembly, dbsnp_id
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (allele_id) DO UPDATE SET
    variation_id          = EXCLUDED.variation_id,
    name                  = EXCLUDED.name,
    gene_symbol           = EXCLUDED.gene_symbol,
    hgvs_cdna             = EXCLUDED.hgvs_cdna,
    hgvs_protein          = EXCLUDED.hgvs_protein,
    clinical_significance = EXCLUDED.clinical_significance,
    clinsig_simple        = EXCLUDED.clinsig_simple,
    review_status         = EXCLUDED.review_status,
    last_evaluated        = EXCLUDED.last_evaluated,
    phenotype_ids         = EXCLUDED.phenotype_ids,
    phenotype_list        = EXCLUDED.phenotype_list,
    updated_at            = NOW()
"""


def load_variant_summary(conn, path: Path) -> int:
    filename = path.name
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM clinvar.load_log WHERE filename = %s", (filename,))
        if cur.fetchone():
            print(f"  Skipping (already loaded): {filename}")
            return 0

    print(f"  Loading variant_summary: {path}", flush=True)
    count = 0
    batch = []

    with _open(path) as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            if row.get("Assembly") != "GRCh38":
                continue
            batch.append((
                _int_or_none(row.get("AlleleID", "")),
                _int_or_none(row.get("VariationID", "")),
                row.get("Name", "").strip() or None,
                _int_or_none(row.get("GeneID", "")),
                row.get("GeneSymbol", "").split(";")[0].strip() or None,
                row.get("Name", ""),       # hgvs_cdna extracted from Name field
                None,                       # hgvs_protein — populated separately if needed
                row.get("MolecularConsequence", "").strip() or None,
                row.get("ClinicalSignificance", "").strip() or None,
                _int_or_none(row.get("ClinSigSimple", "")),
                row.get("ReviewStatus", "").strip() or None,
                _date_or_none(row.get("LastEvaluated", "")),
                row.get("PhenotypeIDS", "").strip() or None,
                row.get("PhenotypeList", "").strip() or None,
                row.get("Chromosome", "").strip() or None,
                _int_or_none(row.get("Start", "")),
                _int_or_none(row.get("Stop", "")),
                row.get("Assembly", "").strip() or None,
                _int_or_none(row.get("RS# (dbSNP)", "")),
            ))
            count += 1

            if len(batch) >= 1000:
                with conn.cursor() as cur:
                    cur.executemany(VS_UPSERT, batch)
                conn.commit()
                pg.progress(count, None, "  variant_summary")
                batch.clear()

    if batch:
        with conn.cursor() as cur:
            cur.executemany(VS_UPSERT, batch)
        conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO clinvar.load_log (filename, records_loaded) VALUES (%s, %s)"
            " ON CONFLICT (filename) DO UPDATE SET records_loaded = EXCLUDED.records_loaded, loaded_at = NOW()",
            (filename, count),
        )
    conn.commit()
    print(f"\n  Loaded {count:,} variants from {filename}")
    return count


# ── var_citations ─────────────────────────────────────────────────────────────

VC_UPSERT = """
INSERT INTO clinvar.var_citations (allele_id, variation_id, rs_id, citation_source, citation_id)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
"""


def load_var_citations(conn, path: Path) -> int:
    filename = path.name
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM clinvar.load_log WHERE filename = %s", (filename,))
        if cur.fetchone():
            print(f"  Skipping (already loaded): {filename}")
            return 0

    print(f"  Loading var_citations: {path}", flush=True)
    count = 0
    batch = []

    with _open(path) as f:
        for line in f:
            if line.startswith("#"):
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 6:
                continue
            allele_id     = _int_or_none(parts[0])
            variation_id  = _int_or_none(parts[1])
            rs_id         = _int_or_none(parts[2])
            citation_src  = parts[4].strip()
            citation_id   = _int_or_none(parts[5])

            if allele_id is None or citation_id is None:
                continue

            batch.append((allele_id, variation_id, rs_id, citation_src, citation_id))
            count += 1

            if len(batch) >= 2000:
                with conn.cursor() as cur:
                    cur.executemany(VC_UPSERT, batch)
                conn.commit()
                pg.progress(count, None, "  var_citations")
                batch.clear()

    if batch:
        with conn.cursor() as cur:
            cur.executemany(VC_UPSERT, batch)
        conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO clinvar.load_log (filename, records_loaded) VALUES (%s, %s)"
            " ON CONFLICT (filename) DO UPDATE SET records_loaded = EXCLUDED.records_loaded, loaded_at = NOW()",
            (filename, count),
        )
    conn.commit()
    print(f"\n  Loaded {count:,} citations from {filename}")
    return count


# ── Entry point ───────────────────────────────────────────────────────────────

LOADERS = {
    "variant_summary": load_variant_summary,
    "var_citations":   load_var_citations,
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
