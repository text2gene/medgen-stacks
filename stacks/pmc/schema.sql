-- PMC Open Access full-text schema
-- Sources: oa_comm and oa_noncomm JATS XML bundles
-- https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/

CREATE SCHEMA IF NOT EXISTS pmc;

CREATE TABLE IF NOT EXISTS pmc.article (
    pmcid           TEXT            PRIMARY KEY,   -- e.g. "PMC176545"
    pmid            BIGINT,                        -- cross-ref to pubmed.article
    doi             TEXT,
    title           TEXT,
    abstract        TEXT,
    body            TEXT,           -- full text, tags stripped
    journal         TEXT,           -- journal-id nlm-ta or iso-abbrev
    year            SMALLINT,
    license         TEXT,           -- oa_comm vs oa_noncomm
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS pmc_article_pmid_idx   ON pmc.article (pmid)  WHERE pmid IS NOT NULL;
CREATE INDEX IF NOT EXISTS pmc_article_doi_idx    ON pmc.article (doi)   WHERE doi IS NOT NULL;
CREATE INDEX IF NOT EXISTS pmc_article_year_idx   ON pmc.article (year);

ALTER TABLE pmc.article ADD COLUMN IF NOT EXISTS fts TSVECTOR
    GENERATED ALWAYS AS (
        setweight(to_tsvector('english', coalesce(title, '')), 'A') ||
        setweight(to_tsvector('english', coalesce(abstract, '')), 'B') ||
        setweight(to_tsvector('english', coalesce(body, '')), 'C')
    ) STORED;
CREATE INDEX IF NOT EXISTS pmc_article_fts_idx ON pmc.article USING GIN (fts);

-- Track loaded tar.gz files for idempotency
CREATE TABLE IF NOT EXISTS pmc.load_log (
    filename        TEXT            PRIMARY KEY,
    records_loaded  INTEGER,
    loaded_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
