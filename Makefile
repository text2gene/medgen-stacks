SHELL := /usr/bin/env bash

.PHONY: all pubmed pubmed-oa clinvar gene orphanet hpo hgnc medgen disgenet pubtator setup help

all: hpo orphanet hgnc clinvar gene medgen disgenet pubtator pubmed  ## Load all stacks (smallest → largest)

setup:                              ## Check prerequisites and connection
	@source bin/common.sh && \
	    require_commands psql wget python3 && \
	    check_pg_connection && \
	    echo "All prerequisites met."

pubmed:                             ## Full PubMed stack (mirror → schema → load → index)
	@$(MAKE) -C stacks/pubmed all

pubmed-oa:                          ## PubMed Open Access subset only (no NLM creds needed)
	@$(MAKE) -C stacks/pubmed load-oa

clinvar:                            ## Full ClinVar stack (variant_summary + var_citations)
	@$(MAKE) -C stacks/clinvar all

gene:                               ## NCBI Entrez Gene (gene_info + gene2pubmed + gene_history)
	@$(MAKE) -C stacks/gene all

orphanet:                           ## Orphanet rare disease stack (disorders + gene associations)
	@$(MAKE) -C stacks/orphanet all

hpo:                                ## Human Phenotype Ontology (terms + disease/gene annotations)
	@$(MAKE) -C stacks/hpo all

hgnc:                               ## HGNC gene nomenclature (approved symbols + aliases)
	@$(MAKE) -C stacks/hgnc all

medgen:                             ## NCBI MedGen disease concepts + PubMed links
	@$(MAKE) -C stacks/medgen all

disgenet:                           ## DisGeNET gene-disease and variant-disease associations
	@$(MAKE) -C stacks/disgenet all

pubtator:                           ## PubTator text-mined gene/mutation mentions in PubMed
	@$(MAKE) -C stacks/pubtator all

help:                               ## Show this help
	@echo "medgen-stacks — bash-first biomedical database loader"
	@echo ""
	@echo "Usage: make <target>"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | \
	    awk 'BEGIN {FS = ":.*?##"}; {printf "  %-14s %s\n", $$1, $$2}'
	@echo ""
	@echo "Config: copy conf/env.example to .env and fill in credentials."
