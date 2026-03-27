#!/usr/bin/env bash
# mirror.sh — download PubMed/MEDLINE baseline and update files from NLM FTP.
#
# NLM FTP requires registration for the baseline:
#   https://www.nlm.nih.gov/databases/download/pubmed_medline.html
#
# The open-access subset does NOT require registration and is a good starting
# point (~5GB). Set PUBMED_OA_ONLY=1 to download only that.
#
# Usage:
#   ./mirror.sh                  # full baseline + updates (requires NLM creds)
#   PUBMED_OA_ONLY=1 ./mirror.sh # open-access subset only

source "$(dirname "$0")/../../bin/common.sh"
require_commands wget

PUBMED_OA_ONLY="${PUBMED_OA_ONLY:-0}"

NLM_BASELINE_URL="https://ftp.ncbi.nlm.nih.gov/pubmed/baseline"
NLM_UPDATE_URL="https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles"
NLM_OA_URL="https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk"

DEST="${MEDGEN_MIRROR_DIR}/pubmed"
mkdir -p "${DEST}/baseline" "${DEST}/updates"

if [[ "${PUBMED_OA_ONLY}" == "1" ]]; then
    log_info "Downloading PubMed Open Access subset (no NLM credentials required)"
    mirror_ftp_pattern "${NLM_OA_URL}/oa_comm/xml/" "*.xml.gz" "pubmed/oa"
    mirror_ftp_pattern "${NLM_OA_URL}/oa_noncomm/xml/" "*.xml.gz" "pubmed/oa"
    log_ok "OA mirror complete → ${DEST}/oa"
else
    if [[ -z "${NLM_FTP_USER}" || -z "${NLM_FTP_PASS}" ]]; then
        log_error "NLM_FTP_USER and NLM_FTP_PASS must be set for full baseline download. Set PUBMED_OA_ONLY=1 to skip. See https://www.nlm.nih.gov/databases/download/pubmed_medline.html"
    fi

    log_info "Mirroring MEDLINE baseline (~25GB compressed)"
    wget --quiet --show-progress \
         --user="${NLM_FTP_USER}" --password="${NLM_FTP_PASS}" \
         --directory-prefix="${DEST}/baseline" \
         --continue --accept="*.xml.gz" \
         --no-directories --recursive --level=1 \
         "${NLM_BASELINE_URL}/"

    log_info "Mirroring MEDLINE update files"
    wget --quiet --show-progress \
         --user="${NLM_FTP_USER}" --password="${NLM_FTP_PASS}" \
         --directory-prefix="${DEST}/updates" \
         --continue --accept="*.xml.gz" \
         --no-directories --recursive --level=1 \
         "${NLM_UPDATE_URL}/"

    log_ok "Full baseline mirror complete → ${DEST}"
fi
