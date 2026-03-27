"""
Shared PostgreSQL utilities for medgen-stacks Python scripts.

Reads connection config from environment variables (set by common.sh / .env).
"""
import io
import os
import sys

import psycopg2
import psycopg2.extras


def connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=int(os.environ.get("PGPORT", 5432)),
        user=os.environ.get("PGUSER", "medgen"),
        password=os.environ.get("PGPASSWORD", ""),
        dbname=os.environ.get("PGDATABASE", "medgen"),
    )


def copy_records(conn, table: str, columns: list[str], rows) -> int:
    """
    Bulk-insert an iterable of tuples into `table` using COPY.
    Returns the number of rows inserted.
    """
    buf = io.StringIO()
    count = 0
    for row in rows:
        line = "\t".join(
            "" if v is None else str(v).replace("\\", "\\\\").replace("\t", " ").replace("\n", " ")
            for v in row
        )
        buf.write(line + "\n")
        count += 1

    if count == 0:
        return 0

    buf.seek(0)
    col_str = ", ".join(columns)
    with conn.cursor() as cur:
        cur.copy_expert(f"COPY {table} ({col_str}) FROM STDIN", buf)
    conn.commit()
    return count


def progress(current: int, total: int | None, label: str = "", width: int = 40) -> None:
    """Print a simple progress indicator to stderr."""
    if total:
        pct = current / total
        filled = int(width * pct)
        bar = "█" * filled + "░" * (width - filled)
        print(f"\r{label} [{bar}] {current:,}/{total:,}", end="", file=sys.stderr, flush=True)
    else:
        print(f"\r{label} {current:,} records", end="", file=sys.stderr, flush=True)
