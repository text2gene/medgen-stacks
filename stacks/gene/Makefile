SHELL := /usr/bin/env bash
STACK_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

.PHONY: all mirror schema load index clean help

all: mirror schema load index   ## Full setup: mirror → schema → load → index

mirror:                         ## Download NCBI Gene files from NCBI FTP
	@bash $(STACK_DIR)mirror.sh

schema:                         ## Create gene schema and tables
	@source $(STACK_DIR)../../bin/common.sh && \
	    ensure_db_exists && \
	    psql_file $(STACK_DIR)schema.sql && \
	    log_ok "Schema ready."

load:                           ## Load Gene data into PostgreSQL (human only)
	@source $(STACK_DIR)../../bin/common.sh && \
	    log_info "Loading gene.info..." && \
	    $${PYTHON:-python3} $(STACK_DIR)load.py info \
	        $${MEDGEN_MIRROR_DIR}/gene/gene_info.gz && \
	    log_info "Loading gene.pubmed..." && \
	    $${PYTHON:-python3} $(STACK_DIR)load.py pubmed \
	        $${MEDGEN_MIRROR_DIR}/gene/gene2pubmed.gz && \
	    log_info "Loading gene.history..." && \
	    $${PYTHON:-python3} $(STACK_DIR)load.py history \
	        $${MEDGEN_MIRROR_DIR}/gene/gene_history.gz && \
	    log_ok "Gene load complete."

index:                          ## Refresh index statistics
	@source $(STACK_DIR)../../bin/common.sh && \
	    psql_exec "ANALYZE gene.info; ANALYZE gene.pubmed; ANALYZE gene.history;" && \
	    log_ok "Index stats updated."

clean:                          ## Drop gene schema (DESTRUCTIVE)
	@read -p "Drop gene schema? [y/N] " ans && [[ "$$ans" == "y" ]] || exit 1
	@source $(STACK_DIR)../../bin/common.sh && \
	    psql_exec "DROP SCHEMA IF EXISTS gene CASCADE;" && \
	    log_ok "gene schema dropped."

help:
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | \
	    awk 'BEGIN {FS = ":.*?##"}; {printf "  %-12s %s\n", $$1, $$2}'
