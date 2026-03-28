#!/usr/bin/env bash
# mirror.sh — download PubMed/MEDLINE baseline and update files from NLM FTP.
#
# The baseline is freely available (no credentials required):
#   https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/
#
# Usage:
#   ./mirror.sh                  # baseline only (default, no creds needed)
#   PUBMED_UPDATES=1 ./mirror.sh # also mirror daily update files

source "$(dirname "$0")/../../bin/common.sh"
require_commands wget curl

NLM_BASELINE_URL="https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
NLM_UPDATE_URL="https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/"

DEST="${MEDGEN_MIRROR_DIR}/pubmed"
mkdir -p "${DEST}/baseline" "${DEST}/updates"

# Download each .xml.gz explicitly by parsing the directory index.
mirror_nlm_dir() {
    local url="$1"
    local dest="$2"
    local label="$3"

    log_info "Fetching file list from ${url}"
    local files
    files=$(curl -s "${url}" | grep -o 'href="[^"]*\.xml\.gz"' | sed 's/href="//;s/"//')

    local count
    count=$(echo "${files}" | grep -c . || true)
    log_info "Downloading ${count} files → ${dest}"

    while IFS= read -r fname; do
        [[ -z "${fname}" ]] && continue
        local dest_file="${dest}/${fname}"
        if [[ -s "${dest_file}" ]]; then
            continue   # already have it
        fi
        wget --quiet --show-progress -c -O "${dest_file}" "${url}${fname}" || {
            rm -f "${dest_file}"
            log_warn "  Failed: ${fname}"
        }
    done <<< "${files}"
    log_ok "${label} mirror complete ($(ls "${dest}"/*.xml.gz 2>/dev/null | wc -l) files)"
}

log_info "Mirroring MEDLINE baseline (~36GB compressed) → ${DEST}/baseline"
mirror_nlm_dir "${NLM_BASELINE_URL}" "${DEST}/baseline" "baseline"

if [[ "${PUBMED_UPDATES:-0}" == "1" ]]; then
    log_info "Mirroring MEDLINE update files → ${DEST}/updates"
    mirror_nlm_dir "${NLM_UPDATE_URL}" "${DEST}/updates" "updates"
fi
