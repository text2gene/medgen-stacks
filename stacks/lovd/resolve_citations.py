#!/usr/bin/env python3
"""
resolve_citations.py — batch-resolve LOVD citation text to PMIDs.

Reads citation text from lovd.variant.all_fields, parses author+year,
searches local pubmed.article first, then CrossRef as backup. Inserts
resolved PMIDs into lovd.variant_ref.

Run:
    python3 resolve_citations.py              # resolve all unresolved
    python3 resolve_citations.py --dry-run    # parse only, don't write
    python3 resolve_citations.py --crossref   # also try CrossRef for misses
    python3 resolve_citations.py CFTR         # specific gene only

Requires: psycopg2, metapub (for CrossRef backup)
"""

import re
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "python"))
import pg

# Import citation resolver — try metapub first, fall back to local copy
try:
    from metapub.citation_resolver import parse_citation_text, CitationResolver
except (ImportError, ModuleNotFoundError):
    # metapub not installed — use local copy deployed alongside this script
    import importlib.util
    _cr_path = Path(__file__).parent / "citation_resolver.py"
    if _cr_path.exists():
        _spec = importlib.util.spec_from_file_location("citation_resolver", str(_cr_path))
        _mod = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_mod)
        parse_citation_text = _mod.parse_citation_text
        CitationResolver = _mod.CitationResolver
    else:
        raise ImportError(
            "citation_resolver.py not found. Deploy it alongside this script "
            "or install metapub with: pip install metapub"
        )

UPSERT_REF = """
INSERT INTO lovd.variant_ref (gene, hgvs_cdna, source_host, ref_type, ref_id)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
"""


def get_unresolved(conn, gene_filter=None):
    """Get variants with citation text but no PMID in variant_ref."""
    sql = """
    SELECT v.gene, v.hgvs_cdna, v.source_host,
        v.all_fields->>'VariantOnGenome/Reference' AS genome_ref,
        v.all_fields->>'Individual/Reference' AS indiv_ref
    FROM lovd.variant v
    WHERE (
        (v.all_fields->>'VariantOnGenome/Reference' IS NOT NULL
         AND v.all_fields->>'VariantOnGenome/Reference' NOT IN ('-', ''))
        OR
        (v.all_fields->>'Individual/Reference' IS NOT NULL
         AND v.all_fields->>'Individual/Reference' NOT IN ('-', ''))
    )
    AND NOT EXISTS (
        SELECT 1 FROM lovd.variant_ref vr
        WHERE vr.gene = v.gene AND vr.hgvs_cdna = v.hgvs_cdna
          AND vr.source_host = v.source_host AND vr.ref_type = 'pmid'
    )
    """
    params = []
    if gene_filter:
        sql += " AND v.gene = %s"
        params.append(gene_filter)
    sql += " ORDER BY v.gene, v.hgvs_cdna"

    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def main():
    dry_run = "--dry-run" in sys.argv
    use_crossref = "--crossref" in sys.argv
    gene_filter = None
    for a in sys.argv[1:]:
        if not a.startswith("--"):
            gene_filter = a

    conn = pg.connect()

    # Set up resolver — build URL from env vars (same ones pg.connect() uses)
    import os
    db_url = "postgresql://{user}:{password}@{host}:{port}/{db}".format(
        user=os.environ.get("PGUSER", "medgen"),
        password=os.environ.get("PGPASSWORD", "medgen"),
        host=os.environ.get("PGHOST", "127.0.0.1"),
        port=os.environ.get("PGPORT", "5432"),
        db=os.environ.get("PGDATABASE", "medgen"),
    )
    print(f"Resolver DB: {db_url.replace(os.environ.get('PGPASSWORD',''), '***')}", flush=True)
    resolver = CitationResolver(db_url=db_url)
    print("Resolver ready", flush=True)

    print("Connecting to DB...", flush=True)
    rows = get_unresolved(conn, gene_filter)
    print(f"Found {len(rows)} variants with unresolved citations", flush=True)

    # Deduplicate: many variants share the same citation text
    # Resolve unique texts once, then apply to all variants
    text_to_variants: dict[str, list[tuple]] = defaultdict(list)
    for gene, hgvs, host, genome_ref, indiv_ref in rows:
        # Use genome reference first, fall back to individual reference
        ref_text = genome_ref or indiv_ref
        if ref_text and ref_text.strip() not in ("-", ""):
            text_to_variants[(ref_text, gene)].append((gene, hgvs, host))

    unique_texts = list(text_to_variants.keys())
    print(f"Unique citation/gene pairs to resolve: {len(unique_texts)}")
    if dry_run:
        print("(dry run — not writing to DB)")

    resolved = 0
    failed = 0
    skipped = 0
    total_pmids_added = 0

    for i, (ref_text, gene) in enumerate(unique_texts):
        # Parse citation text into author+year
        citations = parse_citation_text(ref_text)
        if not citations:
            skipped += 1
            continue

        found_pmids = []
        for cite in citations:
            result = resolver._resolve_one(cite, gene=gene, use_crossref=use_crossref)
            if result and result.get("pmid"):
                found_pmids.append(result)

        if found_pmids:
            resolved += 1
            variants = text_to_variants[(ref_text, gene)]

            if not dry_run:
                with conn.cursor() as cur:
                    for pmid_result in found_pmids:
                        pmid = str(pmid_result["pmid"])
                        for g, hgvs, host in variants:
                            cur.execute(UPSERT_REF, (g, hgvs, host, "pmid", pmid))
                            total_pmids_added += 1
                conn.commit()

            if (i + 1) % 50 == 0 or i < 5:
                sample = found_pmids[0]
                print(f"  [{i+1}/{len(unique_texts)}] {gene}: "
                      f"'{citations[0].get('author', '?')} {citations[0].get('year', '?')}' "
                      f"-> PMID {sample['pmid']} ({sample['method']}, "
                      f"conf={sample['confidence']:.2f}) "
                      f"[{len(variants)} variants]", flush=True)
        else:
            failed += 1
            if (i + 1) % 200 == 0:
                print(f"  [{i+1}/{len(unique_texts)}] progress: "
                      f"{resolved} resolved, {failed} failed, {skipped} skipped",
                      flush=True)

    print(f"\nDone. {resolved} resolved, {failed} failed, {skipped} skipped.")
    print(f"Total PMIDs added to variant_ref: {total_pmids_added}")

    conn.close()


if __name__ == "__main__":
    main()
