-- Gene schema
-- Sources: gene_info.gz, gene2pubmed.gz, gene_history.gz
-- https://ftp.ncbi.nlm.nih.gov/gene/DATA/

CREATE SCHEMA IF NOT EXISTS gene;

-- Main gene table (human genes only: tax_id = 9606)
CREATE TABLE IF NOT EXISTS gene.info (
    gene_id         INTEGER         PRIMARY KEY,
    tax_id          INTEGER         NOT NULL,
    symbol          TEXT,
    synonyms        TEXT,           -- semicolon-separated list
    description     TEXT,
    type_of_gene    TEXT,
    full_name       TEXT,
    chromosome      TEXT,
    map_location    TEXT,
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS gene_info_symbol_idx  ON gene.info (symbol);
CREATE INDEX IF NOT EXISTS gene_info_tax_idx     ON gene.info (tax_id);

-- Gene → PubMed associations
CREATE TABLE IF NOT EXISTS gene.pubmed (
    gene_id         INTEGER         NOT NULL,
    pmid            INTEGER         NOT NULL,
    tax_id          INTEGER         NOT NULL,
    PRIMARY KEY (gene_id, pmid)
);

CREATE INDEX IF NOT EXISTS gene_pubmed_pmid_idx ON gene.pubmed (pmid);

-- Discontinued/merged gene IDs (for resolving old IDs)
CREATE TABLE IF NOT EXISTS gene.history (
    tax_id                  INTEGER,
    discontinued_gene_id    INTEGER         NOT NULL,
    discontinued_symbol     TEXT,
    current_gene_id         INTEGER,        -- NULL means discontinued with no replacement
    PRIMARY KEY (discontinued_gene_id)
);

CREATE INDEX IF NOT EXISTS gene_history_current_idx ON gene.history (current_gene_id)
    WHERE current_gene_id IS NOT NULL;

-- Track loaded files for idempotency
CREATE TABLE IF NOT EXISTS gene.load_log (
    filename        TEXT            PRIMARY KEY,
    records_loaded  INTEGER,
    loaded_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
