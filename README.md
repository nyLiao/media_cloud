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
uv run ruff format --check .
uv run mypy src
```

`ruff format` also formats Python blocks inside Markdown, so code samples in `PLAN.md` are
part of the gate.

The current implementation establishes repository configuration, the versioned SQLite schema,
deterministic hashing and identifiers (`identity.py`), and the config-derived extraction
contract (`contracts.py`). Search, article acquisition, LLM extraction, and CSV export are
implemented in later pipeline stages described in `PLAN.md`.

`config/topics.yaml` drives behaviour: `extraction.fields` alone determines the JSON Schema
sent to the model, the validation model, and the exported CSV columns, so adding a field is a
YAML edit with no code change.

Media Cloud and LLM query timeouts are configured separately and validated at a minimum of
300 seconds (five minutes).
