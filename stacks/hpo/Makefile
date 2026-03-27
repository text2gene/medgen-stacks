SHELL := /usr/bin/env bash
STACK_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

HPO_BASE := https://github.com/obophenotype/human-phenotype-ontology/releases/latest/download

.PHONY: all mirror schema load clean help

all: mirror schema load

mirror:                         ## Download HPO data files from JAX GitHub releases
	@source $(STACK_DIR)../../bin/common.sh && \
	    log_info "Mirroring HPO..." && \
	    mirror_file $(HPO_BASE)/hp.obo                 hpo && \
	    mirror_file $(HPO_BASE)/phenotype.hpoa         hpo && \
	    mirror_file $(HPO_BASE)/phenotype_to_genes.txt hpo && \
	    log_ok "HPO mirror complete."

schema:                         ## Create hpo schema and tables
	@source $(STACK_DIR)../../bin/common.sh && \
	    ensure_db_exists && \
	    psql_file $(STACK_DIR)schema.sql && \
	    log_ok "Schema ready."

load:                           ## Load HPO data into PostgreSQL
	@source $(STACK_DIR)../../bin/common.sh && \
	    log_info "Loading HPO terms..." && \
	    $${PYTHON:-python3} $(STACK_DIR)load.py terms \
	        $${MEDGEN_MIRROR_DIR}/hpo/hp.obo && \
	    log_info "Loading disease-phenotype annotations..." && \
	    $${PYTHON:-python3} $(STACK_DIR)load.py disease_phenotype \
	        $${MEDGEN_MIRROR_DIR}/hpo/phenotype.hpoa && \
	    log_info "Loading gene-phenotype links..." && \
	    $${PYTHON:-python3} $(STACK_DIR)load.py gene_phenotype \
	        $${MEDGEN_MIRROR_DIR}/hpo/phenotype_to_genes.txt && \
	    log_ok "HPO load complete."

clean:
	@read -p "Drop hpo schema? [y/N] " ans && [[ "$$ans" == "y" ]] || exit 1
	@source $(STACK_DIR)../../bin/common.sh && \
	    psql_exec "DROP SCHEMA IF EXISTS hpo CASCADE;" && \
	    log_ok "hpo schema dropped."

help:
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | \
	    awk 'BEGIN {FS = ":.*?##"}; {printf "  %-12s %s\n", $$1, $$2}'
