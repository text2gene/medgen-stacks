#!/usr/bin/env bash
# mirror.sh — download HPO data files from JAX GitHub releases.
#
# Sources:
#   hp.obo                  — term definitions (OBO format)
#   phenotype.hpoa          — disease-phenotype annotations (TSV)
#   phenotype_to_genes.txt  — gene-phenotype links (TSV)
#
# Usage:
#   ./mirror.sh

source "$(dirname "$0")/../../bin/common.sh"
require_commands wget

HPO_BASE="https://github.com/obophenotype/human-phenotype-ontology/releases/latest/download"

log_info "Mirroring Human Phenotype Ontology data..."

mirror_file "${HPO_BASE}/hp.obo"                 hpo
mirror_file "${HPO_BASE}/phenotype.hpoa"         hpo
mirror_file "${HPO_BASE}/phenotype_to_genes.txt" hpo

log_ok "HPO mirror complete → ${MEDGEN_MIRROR_DIR}/hpo"
