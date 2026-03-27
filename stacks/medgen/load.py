#!/usr/bin/env python3
"""
load.py — load MedGen RRF data files into PostgreSQL.

Usage:
    python3 load.py concepts  <MGCONSO.RRF.gz>
    python3 load.py relations <MGREL.RRF.gz>
    python3 load.py pubmed    <medgen_pubmed_lnk.txt.gz>
"""

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
    return val if val else None


def _int_or_none(val: str) -> int | None:
    val = val.strip()
    try:
        return int(val) if val not in ("", "-1", ".", "na", "NA") else None
    except ValueError:
        return None


# ── concepts ──────────────────────────────────────────────────────────────────
# MGCONSO.RRF columns (pipe-delimited, no header):
# 0:CUI 1:TS 2:LUI 3:STT 4:SUI 5:ISPREF 6:AUI 7:SAUI 8:SCUI 9:SDUI
# 10:SAB 11:TTY 12:CODE 13:STR 14:SRL 15:SUPPRESS 16:CVF

CONCEPT_UPSERT = """
INSERT INTO medgen.concept (cui, name, source, semantic_type)
VALUES (%s, %s, %s, %s)
ON CONFLICT (cui) DO UPDATE SET
    name          = EXCLUDED.name,
    source        = EXCLUDED.source,
    semantic_type = EXCLUDED.semantic_type
"""

CONCEPT_NAME_UPSERT = """
INSERT INTO medgen.concept_name (cui, name, source, is_preferred)
VALUES (%s, %s, %s, %s)
ON CONFLICT (cui, name, source) DO UPDATE SET
    is_preferred = EXCLUDED.is_preferred
"""


def load_concepts(conn, path: Path) -> int:
    filename = path.name
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM medgen.load_log WHERE filename = %s", (filename,))
        if cur.fetchone():
            print(f"  Skipping (already loaded): {filename}")
            return 0

    print(f"  Loading MedGen concepts: {path}", flush=True)
    count = 0
    concept_batch = []
    name_batch = []

    with _open(path) as f:
        for line in f:
            parts = line.rstrip("\n").split("|")
            if len(parts) < 14:
                continue

            cui      = _str_or_none(parts[0])
            ispref   = parts[5].strip()
            sab      = _str_or_none(parts[10])
            name     = _str_or_none(parts[13])

            if not cui or not name:
                continue

            is_preferred = ispref == "Y"

            # All rows go into concept_name
            name_batch.append((cui, name, sab or "", is_preferred))

            # ISPREF='Y' row is the preferred name for medgen.concept
            if is_preferred:
                concept_batch.append((cui, name, sab, None))

            count += 1

            if len(name_batch) >= 1000:
                if concept_batch:
                    with conn.cursor() as cur:
                        cur.executemany(CONCEPT_UPSERT, concept_batch)
                with conn.cursor() as cur:
                    cur.executemany(CONCEPT_NAME_UPSERT, name_batch)
                conn.commit()
                pg.progress(count, None, "  concepts")
                concept_batch.clear()
                name_batch.clear()

    if concept_batch:
        with conn.cursor() as cur:
            cur.executemany(CONCEPT_UPSERT, concept_batch)
    if name_batch:
        with conn.cursor() as cur:
            cur.executemany(CONCEPT_NAME_UPSERT, name_batch)
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO medgen.load_log (filename, records_loaded) VALUES (%s, %s)"
            " ON CONFLICT (filename) DO UPDATE SET records_loaded = EXCLUDED.records_loaded, loaded_at = NOW()",
            (filename, count),
        )
    conn.commit()
    print(f"\n  Loaded {count:,} concept name rows from {filename}")
    return count


# ── relations ─────────────────────────────────────────────────────────────────
# MGREL.RRF columns (pipe-delimited, no header):
# 0:CUI1 1:AUI1 2:STYPE1 3:REL 4:CUI2 5:AUI2 6:STYPE2 7:RELA
# 8:RUI 9:SRUI 10:SAB 11:SL 12:RG 13:DIR 14:SUPPRESS 15:CVF

REL_UPSERT = """
INSERT INTO medgen.concept_rel (cui1, rel, cui2, rela)
VALUES (%s, %s, %s, %s)
ON CONFLICT (cui1, rel, cui2) DO UPDATE SET
    rela = EXCLUDED.rela
"""


def load_relations(conn, path: Path) -> int:
    filename = path.name
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM medgen.load_log WHERE filename = %s", (filename,))
        if cur.fetchone():
            print(f"  Skipping (already loaded): {filename}")
            return 0

    print(f"  Loading MedGen relations: {path}", flush=True)
    count = 0
    batch = []

    with _open(path) as f:
        for line in f:
            parts = line.rstrip("\n").split("|")
            if len(parts) < 8:
                continue

            cui1 = _str_or_none(parts[0])
            rel  = _str_or_none(parts[3])
            cui2 = _str_or_none(parts[4])
            rela = _str_or_none(parts[7])

            if not cui1 or not rel or not cui2:
                continue

            batch.append((cui1, rel, cui2, rela))
            count += 1

            if len(batch) >= 1000:
                with conn.cursor() as cur:
                    cur.executemany(REL_UPSERT, batch)
                conn.commit()
                pg.progress(count, None, "  relations")
                batch.clear()

    if batch:
        with conn.cursor() as cur:
            cur.executemany(REL_UPSERT, batch)
        conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO medgen.load_log (filename, records_loaded) VALUES (%s, %s)"
            " ON CONFLICT (filename) DO UPDATE SET records_loaded = EXCLUDED.records_loaded, loaded_at = NOW()",
            (filename, count),
        )
    conn.commit()
    print(f"\n  Loaded {count:,} concept relations from {filename}")
    return count


# ── pubmed ────────────────────────────────────────────────────────────────────
# medgen_pubmed_lnk.txt.gz columns (tab-delimited, comment header):
# #CUI | name | PMID

PUBMED_UPSERT = """
INSERT INTO medgen.pubmed (cui, name, pmid)
VALUES (%s, %s, %s)
ON CONFLICT (cui, pmid) DO NOTHING
"""


def load_pubmed(conn, path: Path) -> int:
    filename = path.name
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM medgen.load_log WHERE filename = %s", (filename,))
        if cur.fetchone():
            print(f"  Skipping (already loaded): {filename}")
            return 0

    print(f"  Loading MedGen → PubMed links: {path}", flush=True)
    count = 0
    batch = []

    with _open(path) as f:
        for line in f:
            if line.startswith("#"):
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 3:
                continue

            cui  = _str_or_none(parts[0])
            name = _str_or_none(parts[1])
            pmid = _int_or_none(parts[2])

            if not cui or pmid is None:
                continue

            batch.append((cui, name, pmid))
            count += 1

            if len(batch) >= 1000:
                with conn.cursor() as cur:
                    cur.executemany(PUBMED_UPSERT, batch)
                conn.commit()
                pg.progress(count, None, "  pubmed")
                batch.clear()

    if batch:
        with conn.cursor() as cur:
            cur.executemany(PUBMED_UPSERT, batch)
        conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO medgen.load_log (filename, records_loaded) VALUES (%s, %s)"
            " ON CONFLICT (filename) DO UPDATE SET records_loaded = EXCLUDED.records_loaded, loaded_at = NOW()",
            (filename, count),
        )
    conn.commit()
    print(f"\n  Loaded {count:,} concept → PubMed links from {filename}")
    return count


# ── Entry point ───────────────────────────────────────────────────────────────

LOADERS = {
    "concepts":  load_concepts,
    "relations": load_relations,
    "pubmed":    load_pubmed,
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
