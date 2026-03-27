SHELL := /usr/bin/env bash
STACK_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

.PHONY: all mirror schema load index clean help

all: mirror schema load index   ## Full setup: mirror → schema → load → index

mirror:                         ## Download PubTator gene and mutation files (not the full ~3 GB bioconcepts file)
	@bash $(STACK_DIR)mirror.sh

schema:                         ## Create pubtator schema and tables
	@source $(STACK_DIR)../../bin/common.sh && \
	    ensure_db_exists && \
	    psql_file $(STACK_DIR)schema.sql && \
	    log_ok "Schema ready."

load:                           ## Load PubTator gene then mutation mentions into PostgreSQL
	@source $(STACK_DIR)../../bin/common.sh && \
	    log_info "Loading gene mentions..." && \
	    $${PYTHON:-python3} $(STACK_DIR)load.py genes \
	        $${MEDGEN_MIRROR_DIR}/pubtator/gene2pubtatorcentral.gz && \
	    log_info "Loading mutation mentions..." && \
	    $${PYTHON:-python3} $(STACK_DIR)load.py mutations \
	        $${MEDGEN_MIRROR_DIR}/pubtator/mutation2pubtatorcentral.gz && \
	    log_ok "PubTator load complete."

index:                          ## Refresh index statistics
	@source $(STACK_DIR)../../bin/common.sh && \
	    psql_exec "ANALYZE pubtator.gene_mention; ANALYZE pubtator.mutation_mention;" && \
	    log_ok "Index stats updated."

clean:                          ## Drop pubtator schema (DESTRUCTIVE)
	@read -p "Drop pubtator schema? [y/N] " ans && [[ "$$ans" == "y" ]] || exit 1
	@source $(STACK_DIR)../../bin/common.sh && \
	    psql_exec "DROP SCHEMA IF EXISTS pubtator CASCADE;" && \
	    log_ok "pubtator schema dropped."

help:
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | \
	    awk 'BEGIN {FS = ":.*?##"}; {printf "  %-12s %s\n", $$1, $$2}'
	@echo ""
	@echo "  NOTE: bioconcepts2pubtatorcentral.offset.gz (~3 GB) is the full annotation"
	@echo "        file covering all entity types. It is not fetched by 'mirror' but can"
	@echo "        be downloaded manually; see mirror.sh for details."
