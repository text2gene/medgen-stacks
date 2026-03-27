-- DisGeNET schema
-- Sources: curated_gene_disease_associations.tsv.gz, curated_variant_disease_associations.tsv.gz
-- https://www.disgenet.org/downloads

CREATE SCHEMA IF NOT EXISTS disgenet;

-- Gene → disease associations (curated set)
CREATE TABLE IF NOT EXISTS disgenet.gene_disease (
    gene_id         INTEGER         NOT NULL,
    gene_symbol     TEXT,
    disease_id      TEXT            NOT NULL,
    disease_name    TEXT,
    disease_type    TEXT,
    score           NUMERIC(5,4),
    pmids           TEXT,           -- semicolon-separated PubMed IDs
    source          TEXT,
    PRIMARY KEY (gene_id, disease_id)
);

CREATE INDEX IF NOT EXISTS disgenet_gd_gene_symbol_idx  ON disgenet.gene_disease (gene_symbol);
CREATE INDEX IF NOT EXISTS disgenet_gd_disease_id_idx   ON disgenet.gene_disease (disease_id);
CREATE INDEX IF NOT EXISTS disgenet_gd_score_idx        ON disgenet.gene_disease (score);

-- Variant → disease associations (curated set)
CREATE TABLE IF NOT EXISTS disgenet.variant_disease (
    snp_id          TEXT            NOT NULL,
    chromosome      TEXT,
    position        INTEGER,
    disease_id      TEXT            NOT NULL,
    disease_name    TEXT,
    score           NUMERIC(5,4),
    pmids           TEXT,           -- semicolon-separated PubMed IDs
    source          TEXT,
    PRIMARY KEY (snp_id, disease_id)
);

CREATE INDEX IF NOT EXISTS disgenet_vd_snp_id_idx       ON disgenet.variant_disease (snp_id);
CREATE INDEX IF NOT EXISTS disgenet_vd_disease_id_idx   ON disgenet.variant_disease (disease_id);

-- Track loaded files for idempotency
CREATE TABLE IF NOT EXISTS disgenet.load_log (
    filename        TEXT            PRIMARY KEY,
    records_loaded  INTEGER,
    loaded_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
