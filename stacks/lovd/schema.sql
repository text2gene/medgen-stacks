-- LOVD locus-specific database (LSDB) instance registry
-- Source: https://grenada.lumc.nl/LSDB_list/lsdbs (scraped per gene)
-- One row per (gene, database instance).

CREATE SCHEMA IF NOT EXISTS lovd;

CREATE TABLE IF NOT EXISTS lovd.lsdb (
    gene            TEXT            NOT NULL,   -- HGNC symbol
    name            TEXT            NOT NULL,   -- database display name
    url             TEXT            NOT NULL,   -- base URL of the database
    curator         TEXT,                       -- curator name / institution
    n_variants      INTEGER,                    -- reported unique variant count
    db_type         TEXT,                       -- "LOVD 3.X", "ClinVar", "UMD", etc.
    last_seen       DATE            NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (gene, url)
);

CREATE INDEX IF NOT EXISTS lovd_lsdb_gene_idx    ON lovd.lsdb (gene);
CREATE INDEX IF NOT EXISTS lovd_lsdb_db_type_idx ON lovd.lsdb (db_type);
CREATE INDEX IF NOT EXISTS lovd_lsdb_url_idx     ON lovd.lsdb (url);

-- Track scrape runs
CREATE TABLE IF NOT EXISTS lovd.scrape_log (
    gene        TEXT        PRIMARY KEY,
    scraped_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    n_instances INTEGER
);
