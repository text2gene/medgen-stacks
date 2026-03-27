#!/usr/bin/env bash
# mirror.sh — download PubTator Central annotation files from NCBI FTP.
#
# Source: https://ftp.ncbi.nlm.nih.gov/pub/lu/PubTatorCentral/
#
# Files downloaded:
#   gene2pubtatorcentral.gz      — gene mentions across all PubMed abstracts
#   mutation2pubtatorcentral.gz  — mutation/variant mentions (rs numbers, HGVS)
#
# NOTE: bioconcepts2pubtatorcentral.offset.gz (~3 GB) is the full combined
# annotation file covering all entity types. It is not downloaded here as
# the two focused files above are sufficient for most uses. To fetch it,
# run mirror_file manually:
#   mirror_file "${PUBTATOR_FTP}/bioconcepts2pubtatorcentral.offset.gz" pubtator
#
# Usage:
#   ./mirror.sh

source "$(dirname "$0")/../../bin/common.sh"
require_commands wget

PUBTATOR_FTP="https://ftp.ncbi.nlm.nih.gov/pub/lu/PubTatorCentral"

log_info "Mirroring PubTator Central annotation files..."

mirror_file "${PUBTATOR_FTP}/gene2pubtatorcentral.gz"     pubtator
mirror_file "${PUBTATOR_FTP}/mutation2pubtatorcentral.gz" pubtator

log_ok "PubTator mirror complete → ${MEDGEN_MIRROR_DIR}/pubtator"
