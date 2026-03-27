-- ClinVar schema
-- Sources: variant_summary.txt.gz, var_citations.txt, submission_summary.txt.gz

CREATE SCHEMA IF NOT EXISTS clinvar;

CREATE TABLE IF NOT EXISTS clinvar.variant_summary (
    allele_id               INTEGER         PRIMARY KEY,
    variation_id            INTEGER,
    name                    TEXT,
    gene_id                 INTEGER,
    gene_symbol             TEXT,
    hgvs_cdna               TEXT,
    hgvs_protein            TEXT,
    molecular_consequence   TEXT,
    clinical_significance   TEXT,
    clinsig_simple          SMALLINT,       -- 0=benign 1=pathogenic -1=other
    review_status           TEXT,
    last_evaluated          DATE,
    phenotype_ids           TEXT,
    phenotype_list          TEXT,
    chromosome              TEXT,
    start                   INTEGER,
    stop                    INTEGER,
    assembly                TEXT,
    dbsnp_id                BIGINT,
    updated_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS clinvar_vs_gene_idx       ON clinvar.variant_summary (gene_symbol);
CREATE INDEX IF NOT EXISTS clinvar_vs_variation_idx  ON clinvar.variant_summary (variation_id);
CREATE INDEX IF NOT EXISTS clinvar_vs_dbsnp_idx      ON clinvar.variant_summary (dbsnp_id) WHERE dbsnp_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS clinvar_vs_clinsig_idx    ON clinvar.variant_summary (clinsig_simple);
CREATE INDEX IF NOT EXISTS clinvar_vs_hgvs_idx       ON clinvar.variant_summary (hgvs_cdna);

-- Variant → PubMed citations
CREATE TABLE IF NOT EXISTS clinvar.var_citations (
    allele_id       INTEGER         NOT NULL,
    variation_id    INTEGER,
    rs_id           BIGINT,
    citation_source TEXT,
    citation_id     BIGINT,
    PRIMARY KEY (allele_id, citation_source, citation_id)
);

CREATE INDEX IF NOT EXISTS clinvar_vc_allele_idx ON clinvar.var_citations (allele_id);
CREATE INDEX IF NOT EXISTS clinvar_vc_pmid_idx   ON clinvar.var_citations (citation_id)
    WHERE citation_source = 'PubMed';

-- Track loaded files
CREATE TABLE IF NOT EXISTS clinvar.load_log (
    filename        TEXT            PRIMARY KEY,
    records_loaded  INTEGER,
    loaded_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
