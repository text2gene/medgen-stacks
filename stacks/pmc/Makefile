SHELL := /usr/bin/env bash
STACK_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

.PHONY: all mirror schema load index clean help

all: mirror schema load index   ## Full setup: mirror → schema → load → index

mirror:                         ## Download PMC OA JATS bundles (no creds needed)
	@bash $(STACK_DIR)mirror.sh

schema:                         ## Create pmc schema and tables
	@source $(STACK_DIR)../../bin/common.sh && \
	    ensure_db_exists && \
	    psql_file $(STACK_DIR)schema.sql && \
	    log_ok "Schema ready."

load:                           ## Load all mirrored bundles (idempotent)
	@source $(STACK_DIR)../../bin/common.sh && \
	    files=$$(find "$${MEDGEN_MIRROR_DIR}/pmc" -name "*.tar.gz" 2>/dev/null | sort) && \
	    if [[ -z "$$files" ]]; then \
	        log_error "No .tar.gz files found. Run 'make mirror' first."; \
	    fi && \
	    echo "$$files" | xargs $${PYTHON:-python3} $(STACK_DIR)load.py

load-comm:                      ## Load oa_comm bundles only
	@source $(STACK_DIR)../../bin/common.sh && \
	    files=$$(find "$${MEDGEN_MIRROR_DIR}/pmc/oa_comm" -name "*.tar.gz" 2>/dev/null | sort) && \
	    if [[ -z "$$files" ]]; then \
	        log_error "No oa_comm files found. Run 'make mirror' first."; \
	    fi && \
	    echo "$$files" | xargs $${PYTHON:-python3} $(STACK_DIR)load.py

index:                          ## Rebuild FTS indexes (run after bulk load)
	@source $(STACK_DIR)../../bin/common.sh && \
	    psql_exec "ANALYZE pmc.article;" && \
	    log_ok "Index stats updated."

clean:                          ## Drop pmc schema (DESTRUCTIVE)
	@read -p "Drop pmc schema? [y/N] " ans && [[ "$$ans" == "y" ]] || exit 1
	@source $(STACK_DIR)../../bin/common.sh && \
	    psql_exec "DROP SCHEMA IF EXISTS pmc CASCADE;" && \
	    log_ok "pmc schema dropped."

help:
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | \
	    awk 'BEGIN {FS = ":.*?##"}; {printf "  %-12s %s\n", $$1, $$2}'
