#!/usr/bin/env bash
# mirror.sh — download DisGeNET curated association files.
#
# Source: https://www.disgenet.org/downloads
#
# The curated gene-disease and variant-disease sets do not require credentials.
#
# Usage:
#   ./mirror.sh

source "$(dirname "$0")/../../bin/common.sh"
require_commands wget

DISGENET_BASE="https://www.disgenet.org/static/disgenet_ap1/files/downloads"

log_info "Mirroring DisGeNET curated association files..."

mirror_file "${DISGENET_BASE}/curated_gene_disease_associations.tsv.gz"     disgenet
mirror_file "${DISGENET_BASE}/curated_variant_disease_associations.tsv.gz"  disgenet

log_ok "DisGeNET mirror complete → ${MEDGEN_MIRROR_DIR}/disgenet"
