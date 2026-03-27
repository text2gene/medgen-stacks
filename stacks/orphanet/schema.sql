-- Orphanet schema
-- Sources: en_product1.xml (disorders), en_product6.xml (gene-disease associations)

CREATE SCHEMA IF NOT EXISTS orphanet;

CREATE TABLE IF NOT EXISTS orphanet.disorder (
    orpha_code      INTEGER         PRIMARY KEY,
    name            TEXT            NOT NULL,
    disorder_type   TEXT
);

CREATE TABLE IF NOT EXISTS orphanet.gene_association (
    orpha_code          INTEGER     NOT NULL,
    gene_symbol         TEXT        NOT NULL,
    hgnc_id             TEXT,
    omim_id             TEXT,
    association_type    TEXT,
    association_status  TEXT,
    PRIMARY KEY (orpha_code, gene_symbol)
);

CREATE INDEX IF NOT EXISTS orphanet_ga_gene_idx ON orphanet.gene_association (gene_symbol);

-- Track loaded files
CREATE TABLE IF NOT EXISTS orphanet.load_log (
    filename        TEXT            PRIMARY KEY,
    records_loaded  INTEGER,
    loaded_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
