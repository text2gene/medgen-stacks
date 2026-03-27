#!/usr/bin/env bash
# mirror.sh — download NCBI Entrez Gene data files.
#
# Source: https://ftp.ncbi.nlm.nih.gov/gene/DATA/
#
# Downloads all organisms; human-only filtering happens at load time.
#
# Usage:
#   ./mirror.sh

source "$(dirname "$0")/../../bin/common.sh"
require_commands wget

GENE_FTP="https://ftp.ncbi.nlm.nih.gov/gene/DATA"

log_info "Mirroring NCBI Entrez Gene files..."

mirror_file "${GENE_FTP}/gene_info.gz"     gene
mirror_file "${GENE_FTP}/gene2pubmed.gz"   gene
mirror_file "${GENE_FTP}/gene_history.gz"  gene

log_ok "Gene mirror complete → ${MEDGEN_MIRROR_DIR}/gene"
