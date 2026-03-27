SHELL := /usr/bin/env bash

.PHONY: all pubmed clinvar gene orphanet hpo setup help

all: pubmed clinvar                 ## Load all default stacks

setup:                              ## Check prerequisites and connection
	@source bin/common.sh && \
	    require_commands psql wget python3 && \
	    check_pg_connection && \
	    echo "All prerequisites met."

pubmed:                             ## Full PubMed stack (mirror → schema → load → index)
	@$(MAKE) -C stacks/pubmed all

pubmed-oa:                          ## PubMed Open Access subset only (no NLM creds)
	@$(MAKE) -C stacks/pubmed load-oa

clinvar:                            ## Full ClinVar stack
	@$(MAKE) -C stacks/clinvar all

gene:                               ## NCBI Gene stack
	@$(MAKE) -C stacks/gene all

orphanet:                           ## Orphanet rare disease stack
	@$(MAKE) -C stacks/orphanet all

hpo:                                ## Human Phenotype Ontology stack
	@$(MAKE) -C stacks/hpo all

help:                               ## Show this help
	@echo "medgen-stacks — bash-first biomedical database loader"
	@echo ""
	@echo "Usage: make <target>"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | \
	    awk 'BEGIN {FS = ":.*?##"}; {printf "  %-14s %s\n", $$1, $$2}'
	@echo ""
	@echo "Config: copy conf/env.example to .env and fill in credentials."
