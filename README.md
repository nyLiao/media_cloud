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

## Stage 1: Search and Review

Stage 1 reads the configured topic, searches Media Cloud in reproducible date windows, and
persists the result pages before any article fetching begins. Live `estimate` and `search`
commands require the Media Cloud token named by `media_cloud.api_key_env`; pass a different
credential file with `--env-file` when needed.

```bash
# Inspect the request without credentials or a Media Cloud API call.
uv run mc-pipeline estimate --topic revolving_door_ca --dry-run

# Run the real search yourself. It uses one full-range partition, skips estimate,
# makes one attempt per page, and writes a timestamped log.
scripts/run_media_cloud_search.sh --topic revolving_door_ca

# Inspect the persisted metadata afterward.
uv run mc-pipeline review-search --topic revolving_door_ca
```

All three commands accept `--config` and `--db`; `estimate` and `search` also accept
`--env-file` and `--dry-run`. `review-search` writes
`data/review/<topic>-search.csv` by default; use `--output` to choose another CSV path. See
`docs/search-review.md` for the review workflow and exit-code handling.

## Stage 2: Deduplicate and QC

Stage 2 is an offline, deterministic database pass. It applies the topic's language, date,
URL, title-length, and optional domain rules before removing normalized URL and exact-title
duplicates. It never calls Media Cloud, fetches article pages, or loads credentials.

Run it explicitly after reviewing the Stage 1 metadata:

```bash
scripts/run_dedup_qc.sh --topic revolving_door_ca
```

The script writes a timestamped log under `data/logs/`. The underlying command accepts
`--config` and `--db`, and can be rerun safely after changing QC configuration:

```bash
uv run mc-pipeline dedup --topic revolving_door_ca --db data/mc.db
```

Media Cloud standard results contain metadata only for this account. The completed full-study
search stored 7,351 distinct stories in eight pages with zero Media Cloud text values. Stage 3
therefore uses one direct article-page request when implemented and skips blocked, missing, or
short pages; Wayback and retries are off by default.

The LLM extraction contract is intentionally limited to `person_name`, `cohort_name`,
`private_org`, `private_time`, `public_org`, `public_time`, and `jurisdiction`. Preview the exact
minimal prompt and schema without credentials or a network call:

```bash
uv run python scripts/preview_llm_prompt.py --topic revolving_door_ca
```

Long LLM jobs are user-run through `scripts/run_llm_analysis.sh`. The script currently exits
with a clear message because Stage 4's `extract` command has not been implemented yet.

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
deterministic hashing and identifiers (`identity.py`), the config-derived extraction contract
(`contracts.py`), resumable Stage 1 Media Cloud search with a metadata review export, and
topic-scoped Stage 2 deduplication/QC. Article acquisition, LLM extraction, and final case CSV
export are implemented in later stages described in `PLAN.md`.

`config/topics.yaml` drives behaviour: `extraction.fields` alone determines the JSON Schema
sent to the model, the validation model, and the exported CSV columns, so adding a field is a
YAML edit with no code change.

Media Cloud and LLM query timeouts are configured separately and validated at a minimum of
300 seconds (five minutes).
