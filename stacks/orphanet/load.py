#!/usr/bin/env python3
"""
load.py — load Orphanet XML files into PostgreSQL.

Usage:
    python3 load.py disorders        <en_product1.xml>
    python3 load.py gene_associations <en_product6.xml>
"""

import sys
import xml.etree.ElementTree as ET
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "python"))
import pg


# ── disorders (en_product1.xml) ───────────────────────────────────────────────

DISORDER_UPSERT = """
INSERT INTO orphanet.disorder (orpha_code, name, disorder_type)
VALUES (%s, %s, %s)
ON CONFLICT (orpha_code) DO UPDATE SET
    name          = EXCLUDED.name,
    disorder_type = EXCLUDED.disorder_type
"""


def load_disorders(conn, path: Path) -> int:
    filename = path.name
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM orphanet.load_log WHERE filename = %s", (filename,))
        if cur.fetchone():
            print(f"  Skipping (already loaded): {filename}")
            return 0

    print(f"  Loading disorders: {path}", flush=True)
    count = 0
    batch = []

    # iterparse: emit 'end' events for each Disorder element and clear as we go
    context = ET.iterparse(str(path), events=("end",))
    for event, elem in context:
        if elem.tag != "Disorder":
            continue

        # Only top-level Disorder elements have OrphaCode as a direct child
        orpha_code_el = elem.find("OrphaCode")
        if orpha_code_el is None:
            elem.clear()
            continue

        try:
            orpha_code = int(orpha_code_el.text.strip())
        except (ValueError, AttributeError):
            elem.clear()
            continue

        name_el = elem.find("Name")
        name = name_el.text.strip() if name_el is not None and name_el.text else None

        disorder_type_el = elem.find("DisorderType/Name")
        disorder_type = (
            disorder_type_el.text.strip()
            if disorder_type_el is not None and disorder_type_el.text
            else None
        )

        batch.append((orpha_code, name, disorder_type))
        count += 1
        elem.clear()

        if len(batch) >= 500:
            with conn.cursor() as cur:
                cur.executemany(DISORDER_UPSERT, batch)
            conn.commit()
            pg.progress(count, None, "  disorders")
            batch.clear()

    if batch:
        with conn.cursor() as cur:
            cur.executemany(DISORDER_UPSERT, batch)
        conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO orphanet.load_log (filename, records_loaded) VALUES (%s, %s)"
            " ON CONFLICT (filename) DO UPDATE SET"
            " records_loaded = EXCLUDED.records_loaded, loaded_at = NOW()",
            (filename, count),
        )
    conn.commit()
    print(f"\n  Loaded {count:,} disorders from {filename}")
    return count


# ── gene_associations (en_product6.xml) ───────────────────────────────────────

GA_UPSERT = """
INSERT INTO orphanet.gene_association (
    orpha_code, gene_symbol, hgnc_id, omim_id,
    association_type, association_status
) VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (orpha_code, gene_symbol) DO UPDATE SET
    hgnc_id            = EXCLUDED.hgnc_id,
    omim_id            = EXCLUDED.omim_id,
    association_type   = EXCLUDED.association_type,
    association_status = EXCLUDED.association_status
"""


def _extract_ref(gene_el: ET.Element, source: str) -> str | None:
    """Return the Reference text for the given Source in ExternalReferenceList."""
    for ext_ref in gene_el.findall("ExternalReferenceList/ExternalReference"):
        src_el = ext_ref.find("Source")
        ref_el = ext_ref.find("Reference")
        if src_el is not None and src_el.text == source and ref_el is not None:
            return ref_el.text.strip() if ref_el.text else None
    return None


def load_gene_associations(conn, path: Path) -> int:
    filename = path.name
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM orphanet.load_log WHERE filename = %s", (filename,))
        if cur.fetchone():
            print(f"  Skipping (already loaded): {filename}")
            return 0

    print(f"  Loading gene associations: {path}", flush=True)
    count = 0
    batch = []

    # We parse each Disorder element; inside it we iterate its gene associations
    context = ET.iterparse(str(path), events=("end",))
    for event, elem in context:
        if elem.tag != "Disorder":
            continue

        orpha_code_el = elem.find("OrphaCode")
        if orpha_code_el is None:
            elem.clear()
            continue

        try:
            orpha_code = int(orpha_code_el.text.strip())
        except (ValueError, AttributeError):
            elem.clear()
            continue

        for assoc in elem.findall("DisorderGeneAssociationList/DisorderGeneAssociation"):
            gene_el = assoc.find("Gene")
            if gene_el is None:
                continue

            symbol_el = gene_el.find("Symbol")
            gene_symbol = (
                symbol_el.text.strip()
                if symbol_el is not None and symbol_el.text
                else None
            )
            if not gene_symbol:
                continue

            hgnc_id = _extract_ref(gene_el, "HGNC")
            omim_id = _extract_ref(gene_el, "OMIM")

            assoc_type_el = assoc.find("DisorderGeneAssociationType/Name")
            assoc_type = (
                assoc_type_el.text.strip()
                if assoc_type_el is not None and assoc_type_el.text
                else None
            )

            assoc_status_el = assoc.find("DisorderGeneAssociationStatus/Name")
            assoc_status = (
                assoc_status_el.text.strip()
                if assoc_status_el is not None and assoc_status_el.text
                else None
            )

            batch.append((orpha_code, gene_symbol, hgnc_id, omim_id, assoc_type, assoc_status))
            count += 1

        elem.clear()

        if len(batch) >= 500:
            with conn.cursor() as cur:
                cur.executemany(GA_UPSERT, batch)
            conn.commit()
            pg.progress(count, None, "  gene_associations")
            batch.clear()

    if batch:
        with conn.cursor() as cur:
            cur.executemany(GA_UPSERT, batch)
        conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO orphanet.load_log (filename, records_loaded) VALUES (%s, %s)"
            " ON CONFLICT (filename) DO UPDATE SET"
            " records_loaded = EXCLUDED.records_loaded, loaded_at = NOW()",
            (filename, count),
        )
    conn.commit()
    print(f"\n  Loaded {count:,} gene associations from {filename}")
    return count


# ── Entry point ───────────────────────────────────────────────────────────────

LOADERS = {
    "disorders":        load_disorders,
    "gene_associations": load_gene_associations,
}


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <subcommand> <file>")
        print(f"Subcommands: {', '.join(LOADERS)}")
        sys.exit(1)

    subcommand, path = sys.argv[1], Path(sys.argv[2])
    if subcommand not in LOADERS:
        print(f"Unknown subcommand: {subcommand!r}. Choose from: {', '.join(LOADERS)}")
        sys.exit(1)

    if not path.exists():
        print(f"File not found: {path}")
        sys.exit(1)

    conn = pg.connect()
    LOADERS[subcommand](conn, path)
    conn.close()


if __name__ == "__main__":
    main()
