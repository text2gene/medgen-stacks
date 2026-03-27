#!/usr/bin/env bash
# mirror.sh — download Orphadata free XML files.
# Usage: bash mirror.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../bin/common.sh
source "${SCRIPT_DIR}/../../bin/common.sh"

ORPHADATA_BASE="https://www.orphadata.com/data/xml"

log_info "Mirroring Orphanet data..."

mirror_file "${ORPHADATA_BASE}/en_product1.xml" orphanet
mirror_file "${ORPHADATA_BASE}/en_product6.xml" orphanet

log_ok "Orphanet mirror complete."
