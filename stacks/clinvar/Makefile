SHELL := /usr/bin/env bash
STACK_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

CLINVAR_FTP := https://ftp.ncbi.nlm.nih.gov/pub/clinvar/tab_delimited

.PHONY: all mirror schema load index clean help

all: mirror schema load index

mirror:                         ## Download ClinVar TSV files from NCBI FTP
	@source $(STACK_DIR)../../bin/common.sh && \
	    log_info "Mirroring ClinVar..." && \
	    mirror_file $(CLINVAR_FTP)/variant_summary.txt.gz clinvar && \
	    mirror_file $(CLINVAR_FTP)/var_citations.txt clinvar && \
	    mirror_file $(CLINVAR_FTP)/submission_summary.txt.gz clinvar && \
	    log_ok "ClinVar mirror complete."

schema:                         ## Create clinvar schema and tables
	@source $(STACK_DIR)../../bin/common.sh && \
	    ensure_db_exists && \
	    psql_file $(STACK_DIR)schema.sql && \
	    log_ok "Schema ready."

load:                           ## Load ClinVar data into PostgreSQL
	@source $(STACK_DIR)../../bin/common.sh && \
	    log_info "Loading variant_summary..." && \
	    $${PYTHON:-python3} $(STACK_DIR)load.py variant_summary \
	        $${MEDGEN_MIRROR_DIR}/clinvar/variant_summary.txt.gz && \
	    log_info "Loading var_citations..." && \
	    $${PYTHON:-python3} $(STACK_DIR)load.py var_citations \
	        $${MEDGEN_MIRROR_DIR}/clinvar/var_citations.txt && \
	    log_ok "ClinVar load complete."

index:                          ## Refresh index statistics
	@source $(STACK_DIR)../../bin/common.sh && \
	    psql_exec "ANALYZE clinvar.variant_summary; ANALYZE clinvar.var_citations;" && \
	    log_ok "Index stats updated."

clean:
	@read -p "Drop clinvar schema? [y/N] " ans && [[ "$$ans" == "y" ]] || exit 1
	@source $(STACK_DIR)../../bin/common.sh && \
	    psql_exec "DROP SCHEMA IF EXISTS clinvar CASCADE;" && \
	    log_ok "clinvar schema dropped."

help:
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | \
	    awk 'BEGIN {FS = ":.*?##"}; {printf "  %-12s %s\n", $$1, $$2}'
