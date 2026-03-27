-- HGNC (HUGO Gene Nomenclature Committee) schema
-- Source: https://ftp.ebi.ac.uk/pub/databases/genenames/hgnc/tsv/hgnc_complete_set.txt

CREATE SCHEMA IF NOT EXISTS hgnc;

-- Gene records from hgnc_complete_set.txt (Approved entries only)
CREATE TABLE IF NOT EXISTS hgnc.gene (
    hgnc_id             TEXT            PRIMARY KEY,
    symbol              TEXT            NOT NULL,
    name                TEXT,
    locus_group         TEXT,
    locus_type          TEXT,
    status              TEXT,
    location            TEXT,
    alias_symbols       TEXT,           -- pipe-separated
    prev_symbols        TEXT,           -- pipe-separated
    entrez_id           INTEGER,
    ensembl_gene_id     TEXT,
    omim_id             TEXT,
    uniprot_ids         TEXT,
    refseq_accession    TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS hgnc_gene_symbol_idx      ON hgnc.gene (symbol);
CREATE INDEX        IF NOT EXISTS hgnc_gene_entrez_idx      ON hgnc.gene (entrez_id);
CREATE INDEX        IF NOT EXISTS hgnc_gene_omim_idx        ON hgnc.gene (omim_id) WHERE omim_id IS NOT NULL;

-- Track loaded files
CREATE TABLE IF NOT EXISTS hgnc.load_log (
    filename        TEXT            PRIMARY KEY,
    records_loaded  INTEGER,
    loaded_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
