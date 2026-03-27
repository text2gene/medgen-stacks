SHELL := /usr/bin/env bash
STACK_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
MIRROR_DIR ?= $(HOME)/medgen-mirror/pubmed

.PHONY: all mirror schema load index clean help

all: mirror schema load index   ## Full setup: mirror → schema → load → index

mirror:                         ## Download PubMed XML files from NLM FTP
	@source $(STACK_DIR)../../bin/common.sh && log_info "Mirroring PubMed..."
	@bash $(STACK_DIR)mirror.sh

schema:                         ## Create pubmed schema and tables
	@source $(STACK_DIR)../../bin/common.sh && \
	    ensure_db_exists && \
	    psql_file $(STACK_DIR)schema.sql && \
	    log_ok "Schema ready."

load:                           ## Load all mirrored XML files (idempotent)
	@source $(STACK_DIR)../../bin/common.sh && \
	    log_info "Loading PubMed files from $(MIRROR_DIR)" && \
	    files=$$(find $(MIRROR_DIR) -name "*.xml.gz" | sort) && \
	    if [[ -z "$$files" ]]; then \
	        log_error "No .xml.gz files found in $(MIRROR_DIR). Run 'make mirror' first."; \
	    fi && \
	    echo "$$files" | xargs $${PYTHON:-python3} $(STACK_DIR)load.py

load-oa:                        ## Load Open Access subset only (no NLM creds needed)
	@PUBMED_OA_ONLY=1 bash $(STACK_DIR)mirror.sh
	@source $(STACK_DIR)../../bin/common.sh && \
	    files=$$(find $(MEDGEN_MIRROR_DIR)/pubmed/oa -name "*.xml.gz" 2>/dev/null | sort) && \
	    if [[ -z "$$files" ]]; then \
	        log_error "No OA files found. Run 'make mirror' with PUBMED_OA_ONLY=1 first."; \
	    fi && \
	    echo "$$files" | xargs $${PYTHON:-python3} $(STACK_DIR)load.py

index:                          ## Rebuild indexes (run after bulk load)
	@source $(STACK_DIR)../../bin/common.sh && \
	    psql_exec "ANALYZE pubmed.article;" && \
	    log_ok "Index stats updated."

clean:                          ## Drop pubmed schema (DESTRUCTIVE)
	@read -p "Drop pubmed schema? [y/N] " ans && [[ "$$ans" == "y" ]] || exit 1
	@source $(STACK_DIR)../../bin/common.sh && \
	    psql_exec "DROP SCHEMA IF EXISTS pubmed CASCADE;" && \
	    log_ok "pubmed schema dropped."

help:
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | \
	    awk 'BEGIN {FS = ":.*?##"}; {printf "  %-12s %s\n", $$1, $$2}'
