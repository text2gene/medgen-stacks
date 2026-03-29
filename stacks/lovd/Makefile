SHELL := /usr/bin/env bash
STACK_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

.PHONY: all schema scrape refresh clean help

all: schema scrape     ## Full setup: schema → scrape

schema:                ## Create lovd schema and tables
	@source $(STACK_DIR)../../bin/common.sh && \
	    ensure_db_exists && \
	    psql_file $(STACK_DIR)schema.sql && \
	    log_ok "Schema ready."

scrape:                ## Scrape LSDB directory (resumable, skips done genes)
	@source $(STACK_DIR)../../bin/common.sh && \
	    log_info "Scraping grenada.lumc.nl/LSDB_list (~25h at 2s/gene)" && \
	    $${PYTHON:-python3} $(STACK_DIR)scrape.py

scrape-genes:          ## Scrape specific genes: make scrape-genes GENES="CFTR BRCA1"
	@source $(STACK_DIR)../../bin/common.sh && \
	    $${PYTHON:-python3} $(STACK_DIR)scrape.py $(GENES)

refresh:               ## Re-scrape all genes (update n_variants counts)
	@source $(STACK_DIR)../../bin/common.sh && \
	    $${PYTHON:-python3} $(STACK_DIR)scrape.py --refresh

clean:                 ## Drop lovd schema (DESTRUCTIVE)
	@read -p "Drop lovd schema? [y/N] " ans && [[ "$$ans" == "y" ]] || exit 1
	@source $(STACK_DIR)../../bin/common.sh && \
	    psql_exec "DROP SCHEMA IF EXISTS lovd CASCADE;" && \
	    log_ok "lovd schema dropped."

help:
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | \
	    awk 'BEGIN {FS = ":.*?##"}; {printf "  %-14s %s\n", $$1, $$2}'
