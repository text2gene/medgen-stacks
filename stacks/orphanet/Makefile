SHELL := /usr/bin/env bash
STACK_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

ORPHADATA_BASE := https://www.orphadata.com/data/xml

.PHONY: all mirror schema load index clean help

all: mirror schema load index

mirror:                         ## Download Orphanet XML files from Orphadata
	@source $(STACK_DIR)../../bin/common.sh && \
	    log_info "Mirroring Orphanet..." && \
	    mirror_file $(ORPHADATA_BASE)/en_product1.xml orphanet && \
	    mirror_file $(ORPHADATA_BASE)/en_product6.xml orphanet && \
	    log_ok "Orphanet mirror complete."

schema:                         ## Create orphanet schema and tables
	@source $(STACK_DIR)../../bin/common.sh && \
	    ensure_db_exists && \
	    psql_file $(STACK_DIR)schema.sql && \
	    log_ok "Schema ready."

load:                           ## Load Orphanet data into PostgreSQL
	@source $(STACK_DIR)../../bin/common.sh && \
	    log_info "Loading disorders..." && \
	    $${PYTHON:-python3} $(STACK_DIR)load.py disorders \
	        $${MEDGEN_MIRROR_DIR}/orphanet/en_product1.xml && \
	    log_info "Loading gene associations..." && \
	    $${PYTHON:-python3} $(STACK_DIR)load.py gene_associations \
	        $${MEDGEN_MIRROR_DIR}/orphanet/en_product6.xml && \
	    log_ok "Orphanet load complete."

index:                          ## Refresh index statistics
	@source $(STACK_DIR)../../bin/common.sh && \
	    psql_exec "ANALYZE orphanet.disorder; ANALYZE orphanet.gene_association;" && \
	    log_ok "Index stats updated."

clean:
	@read -p "Drop orphanet schema? [y/N] " ans && [[ "$$ans" == "y" ]] || exit 1
	@source $(STACK_DIR)../../bin/common.sh && \
	    psql_exec "DROP SCHEMA IF EXISTS orphanet CASCADE;" && \
	    log_ok "orphanet schema dropped."

help:
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | \
	    awk 'BEGIN {FS = ":.*?##"}; {printf "  %-12s %s\n", $$1, $$2}'
