-- PubMed / MEDLINE schema
-- Two complementary representations of each article:
--   xml       — raw NLM PubMed XML; feed directly to metapub's parser
--   structured columns — extracted at load time for fast indexed queries

CREATE SCHEMA IF NOT EXISTS pubmed;

CREATE TABLE IF NOT EXISTS pubmed.article (
    pmid            BIGINT          PRIMARY KEY,
    xml             TEXT            NOT NULL,       -- raw NLM PubMed XML
    title           TEXT,
    first_author    TEXT,
    authors         TEXT,           -- semicolon-separated "Last FM"
    journal         TEXT,           -- ISO abbreviation
    journal_full    TEXT,
    year            SMALLINT,
    month           TEXT,
    volume          TEXT,
    issue           TEXT,
    pages           TEXT,
    doi             TEXT,
    pmc_id          TEXT,
    issn            TEXT,
    abstract        TEXT,
    pub_types       TEXT,           -- semicolon-separated publication types
    mesh_terms      TEXT,           -- semicolon-separated MeSH headings
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Fast PMID lookup (already PK, but explicit for clarity)
-- Year range queries
CREATE INDEX IF NOT EXISTS pubmed_article_year_idx ON pubmed.article (year);
-- Journal browsing
CREATE INDEX IF NOT EXISTS pubmed_article_journal_idx ON pubmed.article (journal);
-- DOI lookup
CREATE INDEX IF NOT EXISTS pubmed_article_doi_idx ON pubmed.article (doi) WHERE doi IS NOT NULL;
-- PMC lookup
CREATE INDEX IF NOT EXISTS pubmed_article_pmc_idx ON pubmed.article (pmc_id) WHERE pmc_id IS NOT NULL;
-- Full-text search on title + abstract
ALTER TABLE pubmed.article ADD COLUMN IF NOT EXISTS fts TSVECTOR
    GENERATED ALWAYS AS (
        setweight(to_tsvector('english', coalesce(title, '')), 'A') ||
        setweight(to_tsvector('english', coalesce(abstract, '')), 'B')
    ) STORED;
CREATE INDEX IF NOT EXISTS pubmed_article_fts_idx ON pubmed.article USING GIN (fts);

-- Track which baseline/update files have been loaded
CREATE TABLE IF NOT EXISTS pubmed.load_log (
    filename        TEXT            PRIMARY KEY,
    records_loaded  INTEGER,
    loaded_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
