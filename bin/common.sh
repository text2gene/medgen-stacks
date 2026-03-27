#!/usr/bin/env bash
# common.sh — shared functions sourced by all stack scripts.
# Source this at the top of any stack script:
#   source "$(dirname "$0")/../../bin/common.sh"

set -euo pipefail

# ── Load .env ────────────────────────────────────────────────────────────────

MEDGEN_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "${MEDGEN_ROOT}/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "${MEDGEN_ROOT}/.env"
    set +a
elif [[ -f "${MEDGEN_ROOT}/conf/env.example" ]]; then
    echo "WARNING: no .env found. Copy conf/env.example to .env and fill in values." >&2
fi

# Defaults
PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-medgen}"
PGDATABASE="${PGDATABASE:-medgen}"
MEDGEN_MIRROR_DIR="${MEDGEN_MIRROR_DIR:-${HOME}/medgen-mirror}"
PYTHON="${PYTHON:-python3}"

export PGHOST PGPORT PGUSER PGDATABASE PGPASSWORD MEDGEN_MIRROR_DIR PYTHON

# ── Logging ───────────────────────────────────────────────────────────────────

_log() { echo "[$(date '+%H:%M:%S')] $*" >&2; }
log_info()  { _log "INFO  $*"; }
log_ok()    { _log "OK    $*"; }
log_warn()  { _log "WARN  $*"; }
log_error() { _log "ERROR $*"; exit 1; }

# ── PostgreSQL helpers ────────────────────────────────────────────────────────

psql_cmd() {
    psql \
        --host="${PGHOST}" \
        --port="${PGPORT}" \
        --username="${PGUSER}" \
        --dbname="${PGDATABASE}" \
        --no-password \
        "$@"
}

# Run a SQL file
psql_file() {
    local file="$1"
    log_info "Running SQL: ${file}"
    psql_cmd --file="${file}"
}

# Run an inline SQL string
psql_exec() {
    psql_cmd --command="$1"
}

# Bulk load a file into a table via COPY
# Usage: pg_copy <table> <file> [<format>] [<options>]
#   format defaults to CSV; pass "text" or "binary" if needed
pg_copy() {
    local table="$1"
    local file="$2"
    local format="${3:-csv}"
    local options="${4:-HEADER}"
    log_info "COPY ${table} ← ${file}"
    psql_cmd --command="\COPY ${table} FROM '${file}' WITH (FORMAT ${format}, ${options})"
}

# ── Download helpers ──────────────────────────────────────────────────────────

# Mirror a single URL into MEDGEN_MIRROR_DIR/<subdir>/
# Skips download if file already exists and --force is not set.
mirror_file() {
    local url="$1"
    local subdir="$2"
    local dest="${MEDGEN_MIRROR_DIR}/${subdir}"
    mkdir -p "${dest}"
    local filename
    filename="$(basename "${url}")"
    if [[ -f "${dest}/${filename}" && -s "${dest}/${filename}" && "${FORCE_DOWNLOAD:-}" != "1" ]]; then
        log_info "Already mirrored: ${dest}/${filename}"
    else
        log_info "Downloading: ${url}"
        # Remove any 0-byte remnant from a previous failed download
        [[ -f "${dest}/${filename}" ]] && ! [[ -s "${dest}/${filename}" ]] && rm -f "${dest}/${filename}"
        wget --show-progress --directory-prefix="${dest}" \
             --continue "${url}" || log_error "wget failed for ${url}"
    fi
    echo "${dest}/${filename}"
}

# Mirror all files matching a pattern from an FTP directory listing
mirror_ftp_pattern() {
    local base_url="$1"
    local pattern="$2"
    local subdir="$3"
    local dest="${MEDGEN_MIRROR_DIR}/${subdir}"
    mkdir -p "${dest}"
    log_info "Mirroring ${pattern} from ${base_url}"
    wget --quiet --show-progress --directory-prefix="${dest}" \
         --continue --accept="${pattern}" \
         --no-directories --recursive --level=1 \
         "${base_url}"
}

# ── Setup check ───────────────────────────────────────────────────────────────

require_commands() {
    for cmd in "$@"; do
        if ! command -v "${cmd}" &>/dev/null; then
            log_error "Required command not found: ${cmd}"
        fi
    done
}

check_pg_connection() {
    if ! psql_cmd --command="SELECT 1" &>/dev/null; then
        log_error "Cannot connect to PostgreSQL at ${PGHOST}:${PGPORT} as ${PGUSER}. Check your .env."
    fi
    log_ok "PostgreSQL connection OK (${PGHOST}:${PGPORT}/${PGDATABASE})"
}

ensure_db_exists() {
    # Create the database if it doesn't exist (connects to 'postgres' db to do so)
    if ! psql \
            --host="${PGHOST}" --port="${PGPORT}" \
            --username="${PGUSER}" --dbname="postgres" \
            --no-password --tuples-only \
            --command="SELECT 1 FROM pg_database WHERE datname='${PGDATABASE}'" \
            2>/dev/null | grep -q 1; then
        log_info "Creating database: ${PGDATABASE}"
        psql \
            --host="${PGHOST}" --port="${PGPORT}" \
            --username="${PGUSER}" --dbname="postgres" \
            --no-password \
            --command="CREATE DATABASE ${PGDATABASE} ENCODING 'UTF8';"
    fi
}
