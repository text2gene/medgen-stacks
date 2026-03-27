#!/usr/bin/env bash
# mirror.sh — download MedGen data files from NCBI FTP.
#
# Source: https://ftp.ncbi.nlm.nih.gov/pub/medgen/
#
# Files:
#   MGCONSO.RRF.gz          — concept names and sources (pipe-delimited RRF)
#   MGREL.RRF.gz            — concept relationships (pipe-delimited RRF)
#   MGSTY.RRF.gz            — semantic types (pipe-delimited RRF)
#   medgen_pubmed_lnk.txt.gz — MedGen concept → PubMed links (tab-delimited)
#
# Usage:
#   ./mirror.sh

source "$(dirname "$0")/../../bin/common.sh"
require_commands wget

MEDGEN_FTP="https://ftp.ncbi.nlm.nih.gov/pub/medgen"

log_info "Mirroring MedGen data files..."

mirror_file "${MEDGEN_FTP}/MGCONSO.RRF.gz"           medgen
mirror_file "${MEDGEN_FTP}/MGREL.RRF.gz"             medgen
mirror_file "${MEDGEN_FTP}/MGSTY.RRF.gz"             medgen
mirror_file "${MEDGEN_FTP}/medgen_pubmed_lnk.txt.gz" medgen

log_ok "MedGen mirror complete → ${MEDGEN_MIRROR_DIR}/medgen"
