#!/usr/bin/env bash
# mirror.sh — download PMC Open Access JATS XML bundles.
#
# Sources (no credentials required):
#   oa_comm    — commercially reusable OA articles
#   oa_noncomm — non-commercial OA articles
#
# Usage:
#   ./mirror.sh              # both comm + noncomm (default)
#   PMC_COMM_ONLY=1 ./mirror.sh

source "$(dirname "$0")/../../bin/common.sh"
require_commands wget curl

BASE="https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk"
DEST="${MEDGEN_MIRROR_DIR}/pmc"
mkdir -p "${DEST}/oa_comm" "${DEST}/oa_noncomm"

# Download each tar.gz explicitly by parsing the directory index.
# wget --recursive doesn't reliably follow links on HTTPS FTP indexes.
mirror_pmc_subset() {
    local url="$1"
    local dest="$2"
    local label="$3"

    log_info "Fetching file list from ${url}"
    local files
    files=$(curl -s "${url}" | grep -o 'href="[^"]*\.tar\.gz"' | sed 's/href="//;s/"//')

    local count
    count=$(echo "${files}" | wc -l)
    log_info "Downloading ${count} bundles → ${dest}"

    while IFS= read -r fname; do
        [[ -z "${fname}" ]] && continue
        local dest_file="${dest}/${fname}"
        if [[ -s "${dest_file}" ]]; then
            log_info "  Already have: ${fname}"
            continue
        fi
        log_info "  Fetching: ${fname}"
        wget --quiet --show-progress -c -O "${dest_file}" "${url}${fname}" || {
            rm -f "${dest_file}"   # remove partial
            log_warn "  Failed: ${fname}"
        }
    done <<< "${files}"
    log_ok "${label} mirror complete ($(ls "${dest}"/*.tar.gz 2>/dev/null | wc -l) files)"
}

mirror_pmc_subset "${BASE}/oa_comm/xml/" "${DEST}/oa_comm" "oa_comm"

if [[ "${PMC_COMM_ONLY:-0}" != "1" ]]; then
    mirror_pmc_subset "${BASE}/oa_noncomm/xml/" "${DEST}/oa_noncomm" "oa_noncomm"
fi
