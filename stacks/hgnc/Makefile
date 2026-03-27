SHELL := /usr/bin/env bash
STACK_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

HGNC_BASE := https://ftp.ebi.ac.uk/pub/databases/genenames/hgnc/tsv

.PHONY: all mirror schema load index clean help

all: mirror schema load index

mirror:                         ## Download HGNC complete gene set from EBI FTP
	@source $(STACK_DIR)../../bin/common.sh && \
	    log_info "Mirroring HGNC..." && \
	    mirror_file $(HGNC_BASE)/hgnc_complete_set.txt hgnc && \
	    log_ok "HGNC mirror complete."

schema:                         ## Create hgnc schema and tables
	@source $(STACK_DIR)../../bin/common.sh && \
	    ensure_db_exists && \
	    psql_file $(STACK_DIR)schema.sql && \
	    log_ok "Schema ready."

load:                           ## Load HGNC genes into PostgreSQL
	@source $(STACK_DIR)../../bin/common.sh && \
	    log_info "Loading HGNC genes..." && \
	    $${PYTHON:-python3} $(STACK_DIR)load.py genes \
	        $${MEDGEN_MIRROR_DIR}/hgnc/hgnc_complete_set.txt && \
	    log_ok "HGNC load complete."

index:                          ## Refresh index statistics
	@source $(STACK_DIR)../../bin/common.sh && \
	    psql_exec "ANALYZE hgnc.gene;" && \
	    log_ok "Index stats updated."

clean:
	@read -p "Drop hgnc schema? [y/N] " ans && [[ "$$ans" == "y" ]] || exit 1
	@source $(STACK_DIR)../../bin/common.sh && \
	    psql_exec "DROP SCHEMA IF EXISTS hgnc CASCADE;" && \
	    log_ok "hgnc schema dropped."

help:
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | \
	    awk 'BEGIN {FS = ":.*?##"}; {printf "  %-12s %s\n", $$1, $$2}'
