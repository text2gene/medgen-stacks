SHELL := /usr/bin/env bash
STACK_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

MEDGEN_FTP := https://ftp.ncbi.nlm.nih.gov/pub/medgen

.PHONY: all mirror schema load index clean help

all: mirror schema load index

mirror:                         ## Download MedGen data files from NCBI FTP
	@source $(STACK_DIR)../../bin/common.sh && \
	    log_info "Mirroring MedGen..." && \
	    mirror_file $(MEDGEN_FTP)/MGCONSO.RRF.gz           medgen && \
	    mirror_file $(MEDGEN_FTP)/MGREL.RRF.gz             medgen && \
	    mirror_file $(MEDGEN_FTP)/MGSTY.RRF.gz             medgen && \
	    mirror_file $(MEDGEN_FTP)/medgen_pubmed_lnk.txt.gz medgen && \
	    log_ok "MedGen mirror complete."

schema:                         ## Create medgen schema and tables
	@source $(STACK_DIR)../../bin/common.sh && \
	    ensure_db_exists && \
	    psql_file $(STACK_DIR)schema.sql && \
	    log_ok "Schema ready."

load:                           ## Load MedGen data into PostgreSQL
	@source $(STACK_DIR)../../bin/common.sh && \
	    log_info "Loading MedGen concepts..." && \
	    $${PYTHON:-python3} $(STACK_DIR)load.py concepts \
	        $${MEDGEN_MIRROR_DIR}/medgen/MGCONSO.RRF.gz && \
	    log_info "Loading MedGen relations..." && \
	    $${PYTHON:-python3} $(STACK_DIR)load.py relations \
	        $${MEDGEN_MIRROR_DIR}/medgen/MGREL.RRF.gz && \
	    log_info "Loading MedGen → PubMed links..." && \
	    $${PYTHON:-python3} $(STACK_DIR)load.py pubmed \
	        $${MEDGEN_MIRROR_DIR}/medgen/medgen_pubmed_lnk.txt.gz && \
	    log_ok "MedGen load complete."

index:                          ## Refresh index statistics
	@source $(STACK_DIR)../../bin/common.sh && \
	    psql_exec "ANALYZE medgen.concept; ANALYZE medgen.concept_name; ANALYZE medgen.concept_rel; ANALYZE medgen.pubmed;" && \
	    log_ok "Index stats updated."

clean:
	@read -p "Drop medgen schema? [y/N] " ans && [[ "$$ans" == "y" ]] || exit 1
	@source $(STACK_DIR)../../bin/common.sh && \
	    psql_exec "DROP SCHEMA IF EXISTS medgen CASCADE;" && \
	    log_ok "medgen schema dropped."

help:
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | \
	    awk 'BEGIN {FS = ":.*?##"}; {printf "  %-12s %s\n", $$1, $$2}'
