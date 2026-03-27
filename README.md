# medgen-stacks

Bash-first loader for biomedical reference databases into PostgreSQL.

Spiritual successor to [medgen-mysql](https://github.com/text2gene/medgen-mysql) —
same philosophy (clone, configure, `make`), rebuilt for PostgreSQL with Python 3
handling the complex parsing where bash isn't the right tool.

## Stacks

| Stack | Source | Size | Notes |
|-------|--------|------|-------|
| `pubmed` | NLM MEDLINE baseline + updates | ~25 GB compressed | NLM registration required for full baseline; OA subset is open |
| `clinvar` | NCBI ClinVar FTP | ~500 MB | No registration needed |
| `gene` | NCBI Entrez Gene | ~1 GB | No registration needed |
| `orphanet` | Orphanet XML | ~200 MB | No registration needed |
| `hpo` | Human Phenotype Ontology | ~50 MB | No registration needed |

Each stack is independent — install only what you need.

## Quick start

```bash
# 1. Clone
git clone https://github.com/text2gene/medgen-stacks.git
cd medgen-stacks

# 2. Configure
cp conf/env.example .env
$EDITOR .env          # set PGHOST, PGUSER, PGPASSWORD, PGDATABASE

# 3. Check prerequisites
make setup

# 4. Load ClinVar (fast, ~5 min)
make clinvar

# 5. Load PubMed Open Access subset (no NLM credentials needed, ~2 hr)
make pubmed-oa

# 6. Load full MEDLINE baseline (requires NLM registration, ~8 hr)
# Set NLM_FTP_USER and NLM_FTP_PASS in .env first
make pubmed
```

## Prerequisites

- PostgreSQL 14+
- `wget`
- Python 3.10+ with `psycopg2` (`pip install psycopg2-binary`)
- Bash 4+

## Design

### Bash-first

Each stack is a directory containing:
- `Makefile` — orchestrates mirror → schema → load → index
- `mirror.sh` — downloads source files from NCBI/NLM FTP
- `schema.sql` — PostgreSQL DDL
- `load.py` or `load.sh` — bulk loading (Python for complex XML, bash+COPY for TSV)

`bin/common.sh` provides shared helpers: logging, `psql_cmd`, `mirror_file`, `pg_copy`.

### Why Python for some loaders?

PubMed XML is complex (nested elements, mixed content, encoding quirks). Python
with `xml.etree.ElementTree` handles this reliably. ClinVar TSV files are
simpler and loaded via bash + PostgreSQL `COPY`.

### Schema design

Each stack gets its own PostgreSQL schema (`pubmed`, `clinvar`, `gene`, etc.)
in a single `medgen` database. Tables are independent; no cross-schema foreign keys.

**PubMed** stores each article twice:
- `xml TEXT` — raw NLM XML, for direct use by metapub (`PubMedArticle(xml)`)
- Structured columns — `title`, `authors`, `journal`, `year`, `doi`, `pmc_id`, `abstract`
  extracted at load time for fast indexed queries and tsvector full-text search

This means you can call `metapub.PubMedArticle(xml)` on any stored article
without hitting the NCBI API.

### Incremental updates

All loaders are idempotent — a `load_log` table tracks which files have been
loaded. Re-running `make load` skips already-loaded files. New MEDLINE update
files are loaded on subsequent runs.

## License

Apache 2.0
