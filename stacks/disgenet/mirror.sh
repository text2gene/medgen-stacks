#!/usr/bin/env bash
# mirror.sh — download DisGeNET curated association files.
#
# NOTE: DisGeNET now requires a registered account for bulk downloads.
# Register at https://www.disgenet.org/signup and set credentials in .env:
#
#   DISGENET_EMAIL=your@email.com
#   DISGENET_PASSWORD=yourpassword
#
# Then run:
#   DISGENET_EMAIL=... DISGENET_PASSWORD=... ./mirror.sh
#
# Alternatively, log in at https://www.disgenet.org/downloads and download
# the files manually into ${MEDGEN_MIRROR_DIR}/disgenet/:
#   - curated_gene_disease_associations.tsv.gz
#   - curated_variant_disease_associations.tsv.gz
#
# Usage:
#   ./mirror.sh

source "$(dirname "$0")/../../bin/common.sh"
require_commands wget

DISGENET_BASE="https://www.disgenet.org/static/disgenet_ap1/files/downloads"
DEST="${MEDGEN_MIRROR_DIR}/disgenet"

mkdir -p "${DEST}"

# Check if files already exist (e.g. manually downloaded)
if [[ -s "${DEST}/curated_gene_disease_associations.tsv.gz" && \
      -s "${DEST}/curated_variant_disease_associations.tsv.gz" ]]; then
    log_ok "DisGeNET files already present → ${DEST}"
    exit 0
fi

# Try authenticated download if credentials are set
if [[ -n "${DISGENET_EMAIL}" && -n "${DISGENET_PASSWORD}" ]]; then
    log_info "Attempting authenticated DisGeNET download..."

    # Get auth token
    TOKEN=$(curl -s -X POST "https://www.disgenet.org/api/auth/" \
        -H "Content-Type: application/json" \
        -d "{\"email\":\"${DISGENET_EMAIL}\",\"password\":\"${DISGENET_PASSWORD}\"}" \
        | python3 -c "import sys,json; print(json.load(sys.stdin).get('token',''))" 2>/dev/null)

    if [[ -z "${TOKEN}" ]]; then
        log_error "DisGeNET authentication failed. Check DISGENET_EMAIL and DISGENET_PASSWORD."
    fi

    for file in curated_gene_disease_associations.tsv.gz curated_variant_disease_associations.tsv.gz; do
        log_info "Downloading: ${DISGENET_BASE}/${file}"
        wget --quiet --show-progress \
             --header="Authorization: Bearer ${TOKEN}" \
             -O "${DEST}/${file}" \
             "${DISGENET_BASE}/${file}" || log_error "Download failed for ${file}"
    done

    log_ok "DisGeNET mirror complete → ${DEST}"
else
    log_error "DisGeNET requires registration. Set DISGENET_EMAIL and DISGENET_PASSWORD in .env, or download files manually to ${DEST}/. See comments in this file for details."
fi
