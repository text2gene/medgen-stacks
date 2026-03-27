#!/usr/bin/env bash
# mirror.sh — download HGNC complete gene set from EBI FTP.
#
# Source:
#   hgnc_complete_set.txt  — full gene set (TSV, no credentials required)
#
# Usage:
#   ./mirror.sh

source "$(dirname "$0")/../../bin/common.sh"
require_commands wget

HGNC_BASE="https://ftp.ebi.ac.uk/pub/databases/genenames/hgnc/tsv"

log_info "Mirroring HGNC complete gene set..."

mirror_file "${HGNC_BASE}/hgnc_complete_set.txt" hgnc

log_ok "HGNC mirror complete → ${MEDGEN_MIRROR_DIR}/hgnc"
