SHELL := /usr/bin/env bash
STACK_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

.PHONY: all mirror schema load index clean help

all: mirror schema load index   ## Full setup: mirror → schema → load → index

mirror:                         ## Download DisGeNET curated files
	@bash $(STACK_DIR)mirror.sh

schema:                         ## Create disgenet schema and tables
	@source $(STACK_DIR)../../bin/common.sh && \
	    ensure_db_exists && \
	    psql_file $(STACK_DIR)schema.sql && \
	    log_ok "Schema ready."

load:                           ## Load DisGeNET data into PostgreSQL
	@source $(STACK_DIR)../../bin/common.sh && \
	    log_info "Loading gene_disease..." && \
	    $${PYTHON:-python3} $(STACK_DIR)load.py gene_disease \
	        $${MEDGEN_MIRROR_DIR}/disgenet/curated_gene_disease_associations.tsv.gz && \
	    log_info "Loading variant_disease..." && \
	    $${PYTHON:-python3} $(STACK_DIR)load.py variant_disease \
	        $${MEDGEN_MIRROR_DIR}/disgenet/curated_variant_disease_associations.tsv.gz && \
	    log_ok "DisGeNET load complete."

index:                          ## Refresh index statistics
	@source $(STACK_DIR)../../bin/common.sh && \
	    psql_exec "ANALYZE disgenet.gene_disease; ANALYZE disgenet.variant_disease;" && \
	    log_ok "Index stats updated."

clean:                          ## Drop disgenet schema (DESTRUCTIVE)
	@read -p "Drop disgenet schema? [y/N] " ans && [[ "$$ans" == "y" ]] || exit 1
	@source $(STACK_DIR)../../bin/common.sh && \
	    psql_exec "DROP SCHEMA IF EXISTS disgenet CASCADE;" && \
	    log_ok "disgenet schema dropped."

help:
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | \
	    awk 'BEGIN {FS = ":.*?##"}; {printf "  %-12s %s\n", $$1, $$2}'
