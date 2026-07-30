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
URL, title-length, configured title terms, and optional domain rules before removing normalized
URL and exact-title duplicates. The default `qc.exclude_title_terms` excludes `hockey`,
`football`, `soccer`, `basketball`, `baseball`, `nhl`, `nfl`, `cfl`, `nba`, and `mlb`. Matching
uses normalized complete terms, so `footballer` is not excluded. It never calls Media Cloud,
fetches article pages, or loads credentials.

Run it explicitly after reviewing the Stage 1 metadata:

```bash
scripts/run_dedup_qc.sh --topic revolving_door_ca
```

The script writes a timestamped log under `data/logs/`. The underlying command accepts
`--config` and `--db`, and can be rerun safely after changing QC configuration:

```bash
uv run mc-pipeline dedup --topic revolving_door_ca --db data/mc.db
```

Inspect the resulting decisions before Stage 3:

```bash
uv run mc-pipeline review-dedup --topic revolving_door_ca
```

`review-dedup` is read-only, needs no credentials, and writes
`data/review/<topic>-dedup.csv` by default. It reports accepted canonical stories, duplicates by
matching rule, QC rejections, and any reachable stories without a Stage 2 decision. See
`docs/dedup-review.md` for the review workflow.

Media Cloud standard results contain metadata only for this account. The completed full-study
search stored 7,351 distinct stories in eight pages with zero Media Cloud text values. Stage 3
therefore uses one direct article-page request and skips blocked, missing, or short pages;
Wayback and retries are off by default.

## Stage 3: Acquire Article Text

Stage 3 reads only Stage 2 rows with `qc_status='ok'`. Inspect the deterministic priority order
and bounded candidate batch without database writes, filesystem changes, sleeping, robots
requests, or article requests:

```bash
uv run mc-pipeline fetch --topic revolving_door_ca --limit 25 --dry-run
```

Before a live fetch, ensure `fetch.user_agent` contains a real contact address. Run the bounded
user launcher after reviewing the dry run:

```bash
scripts/run_article_fetch.sh --topic revolving_door_ca --limit 25
```

The launcher disables the dynamic progress bar to keep its timestamped log readable. Direct
interactive CLI runs show Rich progress automatically; use `--progress` or `--no-progress` to
override detection. Every selected story emits a structured `fetch_entry` log line in live and
dry-run modes. Raw pages are stored atomically as gzip files under `data/raw_html/`.
Successful text is cached globally by story, while robots denials, blocked pages, unsupported
content, transport failures, and short text remain distinct terminal outcomes.

Repeated requests to the same domain use a fresh random interval between one second and
`fetch.per_domain_delay_s`. The separate `fetch.global_max_rps` token bucket continues to cap the
overall request rate across all domains.

Review only a bounded set of successfully fetched articles without network access or a
full-database export. `--limit` is required, capped at 100, and rows are ordered by newest fetch
activity first:

```bash
uv run mc-pipeline review-fetch --topic revolving_door_ca --limit 25
```

The command writes `data/review/<topic>-fetch.csv` by default and includes the stored extracted
article text directly. Failed or blocked fetches and raw HTML paths are excluded. Use `--output`
to choose another path.

The LLM extraction contract is generated from `topics.<name>.extraction.fields`. Prompts include
article titles and plain labelled delimiters, return only relevant articles, and dynamically pack
stored text below `llm.max_input_tokens`. Failed requests receive three serial retries; exhausted
failures remain retryable at the end of later queues. Preview the exact prompt and schema
without credentials or a network call:

```bash
uv run python scripts/preview_llm_prompt.py --topic revolving_door_ca
```

Long LLM jobs are user-run with an explicit article bound. Each committed serial batch refreshes
the case and article-audit CSV snapshots in `data/out` by default. Live extraction displays a
Rich progress bar by default in a terminal; the launcher forces it on even while logging through
`tee`. Pass `--no-progress` to suppress it:

```bash
scripts/run_llm_analysis.sh --topic revolving_door_ca --limit 25
```

Rebuild the CSV snapshots without an LLM request using:

```bash
uv run mc-pipeline export --topic revolving_door_ca
```

Write a secret-free text file for manual LLM testing. It contains only the system prompt followed
by the user prompt. Reconstruct an exact stored batch that returned no cases:

```bash
uv run mc-pipeline extract-prompt --topic revolving_door_ca --batch-id 16 \
  --output data/review/revolving_door_ca-batch-16-llm-prompts.txt
```

Or preview one newly planned packed batch without credentials, network calls, or database writes:

```bash
uv run mc-pipeline extract-prompt --topic revolving_door_ca --limit 10 --batch 1
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
deterministic hashing and identifiers (`identity.py`), the config-derived extraction contract
(`contracts.py`), resumable Stage 1 Media Cloud search with a metadata review export, and
topic-scoped Stage 2 deduplication/QC, bounded Stage 3 article acquisition, serial token-bounded
Stage 4 extraction, and atomic Stage 5 case/article CSV exports.

`config/topics.yaml` drives behaviour: `extraction.fields` alone determines the JSON Schema
sent to the model, the validation model, and the exported CSV columns, so adding a field is a
YAML edit with no code change.

Media Cloud and LLM query timeouts are configured separately and validated at a minimum of
300 seconds (five minutes).
