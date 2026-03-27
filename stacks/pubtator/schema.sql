-- PubTator schema
-- Sources: gene2pubtatorcentral.gz, mutation2pubtatorcentral.gz
-- https://ftp.ncbi.nlm.nih.gov/pub/lu/PubTatorCentral/

CREATE SCHEMA IF NOT EXISTS pubtator;

-- Gene mentions in PubMed abstracts
CREATE TABLE IF NOT EXISTS pubtator.gene_mention (
    pmid        BIGINT          NOT NULL,
    gene_id     BIGINT          NOT NULL,
    mentions    TEXT,
    resource    TEXT,
    PRIMARY KEY (pmid, gene_id)
);

CREATE INDEX IF NOT EXISTS pubtator_gm_gene_id_idx  ON pubtator.gene_mention (gene_id);
CREATE INDEX IF NOT EXISTS pubtator_gm_pmid_idx     ON pubtator.gene_mention (pmid);

-- Mutation/variant mentions in PubMed abstracts
CREATE TABLE IF NOT EXISTS pubtator.mutation_mention (
    pmid        BIGINT          NOT NULL,
    concept_id  TEXT            NOT NULL,
    mentions    TEXT,
    resource    TEXT,
    PRIMARY KEY (pmid, concept_id)
);

CREATE INDEX IF NOT EXISTS pubtator_mm_concept_id_idx   ON pubtator.mutation_mention (concept_id);
CREATE INDEX IF NOT EXISTS pubtator_mm_pmid_idx         ON pubtator.mutation_mention (pmid);

-- Track loaded files for idempotency
CREATE TABLE IF NOT EXISTS pubtator.load_log (
    filename        TEXT            PRIMARY KEY,
    records_loaded  INTEGER,
    loaded_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
