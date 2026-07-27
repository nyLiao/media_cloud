# Media Cloud Pipeline

Reproducible research pipeline for searching Media Cloud, preserving search provenance,
acquiring article text, extracting structured revolving-door cases, and exporting audit-ready
CSV datasets.

## Setup

The repository uses uv and pins the development interpreter to Python 3.12.

```bash
uv sync
cp config/.env.example config/.env
```

Populate `config/.env` locally. It is ignored by Git. `LLM_BASE_URL` is used exactly as
configured; when it already ends in `/v1`, the pipeline does not append another suffix.

Edit `config/topics.yaml`, then initialize the database:

```bash
uv run mc-pipeline init-db --db data/mc.db
```

## Development

```bash
uv run pytest
uv run ruff check .
uv run mypy src
```

The current implementation establishes repository configuration and the versioned SQLite
schema. Search, article acquisition, LLM extraction, and CSV export are implemented in later
pipeline stages described in `PLAN.md`.

