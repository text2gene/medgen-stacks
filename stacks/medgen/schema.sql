-- MedGen schema
-- Sources: MGCONSO.RRF.gz, MGREL.RRF.gz, MGSTY.RRF.gz, medgen_pubmed_lnk.txt.gz

CREATE SCHEMA IF NOT EXISTS medgen;

-- Preferred concept name per CUI
CREATE TABLE IF NOT EXISTS medgen.concept (
    cui             TEXT            PRIMARY KEY,
    name            TEXT,
    source          TEXT,
    semantic_type   TEXT
);

CREATE INDEX IF NOT EXISTS medgen_concept_name_idx ON medgen.concept (name);

-- All names and synonyms for each concept
CREATE TABLE IF NOT EXISTS medgen.concept_name (
    cui             TEXT            NOT NULL,
    name            TEXT            NOT NULL,
    source          TEXT            NOT NULL,
    is_preferred    BOOLEAN,
    PRIMARY KEY (cui, name, source)
);

CREATE INDEX IF NOT EXISTS medgen_concept_name_cui_idx  ON medgen.concept_name (cui);
CREATE INDEX IF NOT EXISTS medgen_concept_name_name_idx ON medgen.concept_name (name);

-- Relationships between concepts
CREATE TABLE IF NOT EXISTS medgen.concept_rel (
    cui1            TEXT            NOT NULL,
    rel             TEXT            NOT NULL,
    cui2            TEXT            NOT NULL,
    rela            TEXT,
    PRIMARY KEY (cui1, rel, cui2)
);

CREATE INDEX IF NOT EXISTS medgen_concept_rel_cui1_idx ON medgen.concept_rel (cui1);
CREATE INDEX IF NOT EXISTS medgen_concept_rel_cui2_idx ON medgen.concept_rel (cui2);

-- Concept → PubMed links
CREATE TABLE IF NOT EXISTS medgen.pubmed (
    cui             TEXT            NOT NULL,
    name            TEXT,
    pmid            BIGINT          NOT NULL,
    PRIMARY KEY (cui, pmid)
);

CREATE INDEX IF NOT EXISTS medgen_pubmed_cui_idx  ON medgen.pubmed (cui);
CREATE INDEX IF NOT EXISTS medgen_pubmed_pmid_idx ON medgen.pubmed (pmid);

-- Track loaded files
CREATE TABLE IF NOT EXISTS medgen.load_log (
    filename        TEXT            PRIMARY KEY,
    records_loaded  INTEGER,
    loaded_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
