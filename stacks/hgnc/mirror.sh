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

# EBI FTP returns 404; use HGNC's Google Cloud Storage bucket instead
HGNC_URL="https://storage.googleapis.com/public-download-files/hgnc/tsv/tsv/hgnc_complete_set.txt"

log_info "Mirroring HGNC complete gene set..."

mirror_file "${HGNC_URL}" hgnc

log_ok "HGNC mirror complete → ${MEDGEN_MIRROR_DIR}/hgnc"
