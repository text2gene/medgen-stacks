#!/usr/bin/env python3
"""
load.py — load HPO data files into PostgreSQL.

Usage:
    python3 load.py terms             <hp.obo>
    python3 load.py disease_phenotype <phenotype.hpoa>
    python3 load.py gene_phenotype    <phenotype_to_genes.txt>
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "python"))
import pg


# ── OBO parser ────────────────────────────────────────────────────────────────

def _parse_obo(path: Path):
    """Yield dicts for each [Term] block in an OBO file.

    Each dict has keys: id, name, definition, is_obsolete, parents (list).
    [Typedef] blocks are skipped.
    """
    in_term = False
    current = {}

    def _emit(rec):
        return {
            "id":          rec.get("id", ""),
            "name":        rec.get("name", ""),
            "definition":  rec.get("definition", None),
            "is_obsolete": rec.get("is_obsolete", False),
            "parents":     rec.get("parents", []),
        }

    with open(path, encoding="utf-8", errors="replace") as f:
        for raw_line in f:
            line = raw_line.rstrip("\n")

            if line == "[Term]":
                if in_term and current.get("id"):
                    yield _emit(current)
                in_term = True
                current = {"parents": []}
                continue

            if line.startswith("[") and line != "[Term]":
                # [Typedef] or other stanzas — flush pending term, stop collecting
                if in_term and current.get("id"):
                    yield _emit(current)
                in_term = False
                current = {}
                continue

            if not in_term:
                continue

            if ":" not in line:
                continue

            tag, _, value = line.partition(": ")
            tag = tag.strip()
            value = value.strip()

            if tag == "id":
                current["id"] = value
            elif tag == "name":
                current["name"] = value
            elif tag == "def":
                # Strip surrounding quotes and trailing dbxref bracket
                # e.g. "Some text." [HP:1234]
                definition = value
                if definition.startswith('"'):
                    end_quote = definition.rfind('"', 1)
                    if end_quote > 0:
                        definition = definition[1:end_quote]
                current["definition"] = definition
            elif tag == "is_obsolete" and value == "true":
                current["is_obsolete"] = True
            elif tag == "is_a":
                # is_a: HP:0000001 ! Root
                parent_id = value.split("!")[0].strip()
                current["parents"].append(parent_id)

    # Emit last block
    if in_term and current.get("id"):
        yield _emit(current)


# ── terms ─────────────────────────────────────────────────────────────────────

TERM_UPSERT = """
INSERT INTO hpo.term (hpo_id, name, definition, is_obsolete)
VALUES (%s, %s, %s, %s)
ON CONFLICT (hpo_id) DO UPDATE SET
    name        = EXCLUDED.name,
    definition  = EXCLUDED.definition,
    is_obsolete = EXCLUDED.is_obsolete
"""

PARENT_UPSERT = """
INSERT INTO hpo.term_parent (hpo_id, parent_id)
VALUES (%s, %s)
ON CONFLICT DO NOTHING
"""


def load_terms(conn, path: Path) -> int:
    filename = path.name
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM hpo.load_log WHERE filename = %s", (filename,))
        if cur.fetchone():
            print(f"  Skipping (already loaded): {filename}")
            return 0

    print(f"  Loading HPO terms: {path}", flush=True)
    term_count = 0
    parent_count = 0
    term_batch = []
    parent_batch = []

    for term in _parse_obo(path):
        term_batch.append((
            term["id"],
            term["name"] or None,
            term["definition"],
            term["is_obsolete"],
        ))
        term_count += 1

        for parent_id in term["parents"]:
            parent_batch.append((term["id"], parent_id))
            parent_count += 1

        if len(term_batch) >= 500:
            with conn.cursor() as cur:
                cur.executemany(TERM_UPSERT, term_batch)
            if parent_batch:
                with conn.cursor() as cur:
                    cur.executemany(PARENT_UPSERT, parent_batch)
            conn.commit()
            pg.progress(term_count, None, "  terms")
            term_batch.clear()
            parent_batch.clear()

    if term_batch:
        with conn.cursor() as cur:
            cur.executemany(TERM_UPSERT, term_batch)
    if parent_batch:
        with conn.cursor() as cur:
            cur.executemany(PARENT_UPSERT, parent_batch)
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO hpo.load_log (filename, records_loaded) VALUES (%s, %s)"
            " ON CONFLICT (filename) DO UPDATE SET records_loaded = EXCLUDED.records_loaded, loaded_at = NOW()",
            (filename, term_count),
        )
    conn.commit()
    print(f"\n  Loaded {term_count:,} terms and {parent_count:,} parent links from {filename}")
    return term_count


# ── disease_phenotype ─────────────────────────────────────────────────────────

DP_UPSERT = """
INSERT INTO hpo.disease_phenotype (disease_id, disease_name, hpo_id, qualifier, evidence, onset, frequency)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (disease_id, hpo_id) DO UPDATE SET
    disease_name = EXCLUDED.disease_name,
    qualifier    = EXCLUDED.qualifier,
    evidence     = EXCLUDED.evidence,
    onset        = EXCLUDED.onset,
    frequency    = EXCLUDED.frequency
"""


def load_disease_phenotype(conn, path: Path) -> int:
    filename = path.name
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM hpo.load_log WHERE filename = %s", (filename,))
        if cur.fetchone():
            print(f"  Skipping (already loaded): {filename}")
            return 0

    print(f"  Loading disease-phenotype annotations: {path}", flush=True)
    count = 0
    batch = []

    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            if line.startswith("#"):
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 4:
                continue

            # Columns: database_id, disease_name, qualifier, hpo_id,
            #          reference, evidence, onset, frequency, sex, modifier, aspect, biocuration
            disease_id   = parts[0].strip() or None
            disease_name = parts[1].strip() or None
            qualifier    = parts[2].strip() or None
            hpo_id       = parts[3].strip() or None
            # parts[4] = reference
            evidence     = parts[5].strip() if len(parts) > 5 else None
            onset        = parts[6].strip() if len(parts) > 6 else None
            frequency    = parts[7].strip() if len(parts) > 7 else None

            if not disease_id or not hpo_id:
                continue

            batch.append((disease_id, disease_name, hpo_id, qualifier or None,
                          evidence or None, onset or None, frequency or None))
            count += 1

            if len(batch) >= 1000:
                with conn.cursor() as cur:
                    cur.executemany(DP_UPSERT, batch)
                conn.commit()
                pg.progress(count, None, "  disease_phenotype")
                batch.clear()

    if batch:
        with conn.cursor() as cur:
            cur.executemany(DP_UPSERT, batch)
        conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO hpo.load_log (filename, records_loaded) VALUES (%s, %s)"
            " ON CONFLICT (filename) DO UPDATE SET records_loaded = EXCLUDED.records_loaded, loaded_at = NOW()",
            (filename, count),
        )
    conn.commit()
    print(f"\n  Loaded {count:,} disease-phenotype annotations from {filename}")
    return count


# ── gene_phenotype ────────────────────────────────────────────────────────────

GP_INSERT = """
INSERT INTO hpo.gene_phenotype (hpo_id, ncbi_gene_id, gene_symbol, disease_id)
VALUES (%s, %s, %s, %s)
"""


def _int_or_none(val: str):
    val = val.strip()
    try:
        return int(val) if val not in ("", "-1", ".", "na", "NA") else None
    except ValueError:
        return None


def load_gene_phenotype(conn, path: Path) -> int:
    filename = path.name
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM hpo.load_log WHERE filename = %s", (filename,))
        if cur.fetchone():
            print(f"  Skipping (already loaded): {filename}")
            return 0

    print(f"  Loading gene-phenotype links: {path}", flush=True)
    count = 0
    batch = []

    # Truncate first since this file has no natural unique key per row
    with conn.cursor() as cur:
        cur.execute("DELETE FROM hpo.gene_phenotype")
    conn.commit()

    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            if line.startswith("#"):
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 4:
                continue

            # Columns: hpo_id, hpo_name, ncbi_gene_id, gene_symbol, disease_id
            hpo_id       = parts[0].strip() or None
            # parts[1] = hpo_name (unused — available in hpo.term)
            ncbi_gene_id = _int_or_none(parts[2]) if len(parts) > 2 else None
            gene_symbol  = parts[3].strip() if len(parts) > 3 else None
            disease_id   = parts[4].strip() if len(parts) > 4 else None

            if not hpo_id:
                continue

            batch.append((hpo_id, ncbi_gene_id, gene_symbol or None, disease_id or None))
            count += 1

            if len(batch) >= 1000:
                with conn.cursor() as cur:
                    cur.executemany(GP_INSERT, batch)
                conn.commit()
                pg.progress(count, None, "  gene_phenotype")
                batch.clear()

    if batch:
        with conn.cursor() as cur:
            cur.executemany(GP_INSERT, batch)
        conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO hpo.load_log (filename, records_loaded) VALUES (%s, %s)"
            " ON CONFLICT (filename) DO UPDATE SET records_loaded = EXCLUDED.records_loaded, loaded_at = NOW()",
            (filename, count),
        )
    conn.commit()
    print(f"\n  Loaded {count:,} gene-phenotype links from {filename}")
    return count


# ── Entry point ───────────────────────────────────────────────────────────────

LOADERS = {
    "terms":              load_terms,
    "disease_phenotype":  load_disease_phenotype,
    "gene_phenotype":     load_gene_phenotype,
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
