-- HPO (Human Phenotype Ontology) schema
-- Sources: hp.obo, phenotype.hpoa, phenotype_to_genes.txt

CREATE SCHEMA IF NOT EXISTS hpo;

-- HPO terms from hp.obo
CREATE TABLE IF NOT EXISTS hpo.term (
    hpo_id          TEXT            PRIMARY KEY,
    name            TEXT,
    definition      TEXT,
    is_obsolete     BOOLEAN         NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS hpo_term_id_idx ON hpo.term (hpo_id);

-- is_a hierarchy from hp.obo
CREATE TABLE IF NOT EXISTS hpo.term_parent (
    hpo_id          TEXT            NOT NULL,
    parent_id       TEXT            NOT NULL,
    PRIMARY KEY (hpo_id, parent_id)
);

CREATE INDEX IF NOT EXISTS hpo_term_parent_hpo_idx    ON hpo.term_parent (hpo_id);
CREATE INDEX IF NOT EXISTS hpo_term_parent_parent_idx ON hpo.term_parent (parent_id);

-- Disease → phenotype annotations from phenotype.hpoa
CREATE TABLE IF NOT EXISTS hpo.disease_phenotype (
    disease_id      TEXT            NOT NULL,
    disease_name    TEXT,
    hpo_id          TEXT            NOT NULL,
    qualifier       TEXT,
    evidence        TEXT,
    onset           TEXT,
    frequency       TEXT,
    PRIMARY KEY (disease_id, hpo_id)
);

CREATE INDEX IF NOT EXISTS hpo_dp_disease_idx ON hpo.disease_phenotype (disease_id);
CREATE INDEX IF NOT EXISTS hpo_dp_hpo_idx     ON hpo.disease_phenotype (hpo_id);

-- Gene → phenotype links from phenotype_to_genes.txt
CREATE TABLE IF NOT EXISTS hpo.gene_phenotype (
    hpo_id          TEXT            NOT NULL,
    ncbi_gene_id    INTEGER,
    gene_symbol     TEXT,
    disease_id      TEXT
);

CREATE INDEX IF NOT EXISTS hpo_gp_hpo_idx    ON hpo.gene_phenotype (hpo_id);
CREATE INDEX IF NOT EXISTS hpo_gp_gene_idx   ON hpo.gene_phenotype (gene_symbol);
CREATE INDEX IF NOT EXISTS hpo_gp_geneid_idx ON hpo.gene_phenotype (ncbi_gene_id);

-- Track loaded files
CREATE TABLE IF NOT EXISTS hpo.load_log (
    filename        TEXT            PRIMARY KEY,
    records_loaded  INTEGER,
    loaded_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
