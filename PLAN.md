# Media Cloud → Structured CSV Pipeline

> **Agent quick start**
>
> - **Status:** foundation implemented (config, DB schema v1, `identity`, `contracts`, CLI `init-db`, probe). Stages 1–5 pending.
> - **Next action:** implement Stage 1 (Search).
> - **Read before editing:** §0 (how to use this doc), §3 (invariants), §5 (data model), then the one stage section you are implementing.
> - **Do not** run a full-range search, crawl, or LLM pass without explicit user approval.
>
> ```bash
> export UV_CACHE_DIR=/tmp/media-cloud-uv-cache
> UV=/opt/homebrew/bin/uv
> $UV sync --python 3.12
> $UV run pytest && $UV run ruff check . && $UV run ruff format --check . && $UV run mypy src
> ```

---

## Contents

| § | Section | Read when |
|---|---|---|
| 0 | [How to use this document](#0-how-to-use-this-document) | always, first |
| 1 | [Objective and scope](#1-objective-and-scope) | onboarding |
| 2 | [Corrections](#2-corrections) | before any schema change |
| 3 | [Invariants](#3-invariants) | always |
| 4 | [Verified baseline](#4-verified-baseline) | before any Media Cloud work |
| 5 | [Data model](#5-data-model) | any DB change |
| 6 | [Stage contracts](#6-stage-contracts) | implementing a stage |
| 7 | [Cross-cutting protocols](#7-cross-cutting-protocols) | always |
| 8 | [Reference](#8-reference) | lookup |
| 9 | [Agent workflow and handoff](#9-agent-workflow-and-handoff) | end of every task |

---

## 0. How to use this document

This file is the implementation contract. Sections are addressable; rules are numbered so
they can be cited in review (`R-SEC-3`, `INV-2`, `S3-R4`).

**Precedence when sources conflict:**

1. explicit user instruction for the current task
2. repository `AGENTS.md` (if added later)
3. this document
4. `docs/api-notes.md`
5. existing implementation patterns and tests

Never silently reinterpret a conflict. Record it in the handoff and make the smallest change
that satisfies the higher-priority source.

**Rule ID prefixes:** `INV-` invariants · `R-SEC-` secrets · `R-DB-` database · `R-ERR-` errors ·
`R-GIT-` git · `R-DEP-` dependencies · `R-CODE-` coding · `R-TEST-` testing · `R-RUN-` run classes ·
`S1-`…`S5-` stage-specific.

**Implementation status** — update this table in the same change that alters it.

| Area | Status | Source of truth |
|---|---|---|
| uv, Python 3.12, lockfile | done | `pyproject.toml`, `uv.lock`, `.python-version` |
| Secret-safe config loading | done | `src/mc_pipeline/config.py` |
| SQLite schema v1 + `init-db` | done (C1–C3, C8 applied) | `src/mc_pipeline/db.py` |
| Canonical hashing, fingerprints, case IDs | done (C4, C7) | `src/mc_pipeline/identity.py` |
| Config → JSON Schema / model / CSV columns | done (C5, C6) | `src/mc_pipeline/contracts.py` |
| Media Cloud probe | done | `scripts/probe_api.py` |
| `errors.py` (typed taxonomy) | done (C9) | `src/mc_pipeline/errors.py` |
| `ratelimit.py` | done | `src/mc_pipeline/ratelimit.py` |
| Stage 1 Search | pending | §6.1 |
| Stage 2 Dedup/QC | pending | §6.2 |
| Stage 3 Fetch | pending | §6.3 |
| Stage 4 Extract | pending | §6.4 |
| Stage 5 Export | pending | §6.5 |

---

## 1. Objective and scope

Build a reproducible, config-driven research pipeline that searches Media Cloud, preserves
query provenance, acquires article text, extracts structured Canadian revolving-door cases,
and exports auditable CSV datasets.

First study: **2021-01-01 … 2025-12-31**, Media Cloud collection `34411583` (`Canada - National`).
Outputs support both named-individual cases and cohort-level findings.

`config/topics.yaml` is the behavioural control surface: adding a topic or an extraction field
is a YAML edit, not a code change. **Multi-topic support is a hard requirement** — several
design rules below exist only because two topics can return the same story.

---

## 2. Corrections

Design review found twelve defects in the foundation. **C1–C12 are fixed** (2026-07-27).
The migration waiver below was exercised and is now **expired**.

### Resolved — C1–C8

| # | Defect | Resolution | Landed in |
|---|---|---|---|
| C1 | QC/dedup verdicts stored on the globally-keyed `stories` row, so two topics returning one story overwrote each other | `qc_status`, `qc_reason`, `dup_of_story_id` moved to `story_topic_state(topic, story_id)`; `stories` is now immutable global metadata | `db.py`, `test_db.py` |
| C2 | `UNIQUE(story_id, prompt_version)` omitted topic, colliding across topics at the same prompt version | `story_extractions.topic` added; constraint is `UNIQUE(topic, story_id, prompt_version)` | `db.py`, `test_db.py` |
| C3 | `NOT NULL`/`CHECK` on model-supplied columns aborted the whole batch on one bad value | Pydantic validates before insert; `validation_status` (`valid`/`invalid`/`unparsed`) + `validation_error` record rejects with zero `cases` rows | `db.py`, `contracts.py`, `test_contracts.py` |
| C4 | `cases.case_id` was `NOT NULL UNIQUE` with no generator | `identity.build_case_id()` — deterministic `topic:story_id:prompt_version:NN`, separator-checked, never model-supplied | `identity.py`, `test_identity.py` |
| C5 | Config, `cases` columns, and the CSV header disagreed on six fields | `contracts.case_csv_columns()` composes prefix + config fields + suffix; `cohort_period`, `previous_sector`, `current_sector` added to config; `link_category` added to topic config | `contracts.py`, `topics.yaml`, `test_contracts.py` |
| C6 | Mapping config `required` onto the JSON Schema `required` array breaks strict mode; `previous_org`/`current_org` were required but absent from cohort rows | `required` now controls nullability in the schema and enforcement in Pydantic; conditional rules declared as `discriminator` + `required_by_discriminator` | `contracts.py`, `config.py`, `topics.yaml` |
| C7 | A fingerprint covering "request controls" would let a `page_size` tweak re-spend quota | `identity.window_fingerprint()` accepts semantic inputs and window bounds only — it has no request-control parameter, and a test asserts passing one is a `TypeError` | `identity.py`, `test_identity.py` |
| C8 | No `busy_timeout`; WAL plus any concurrent reader raised `database is locked` immediately | `PRAGMA busy_timeout = 5000` in `connect_db` | `db.py`, `test_db.py` |

Two design points worth keeping in mind, both now enforced by tests:

- **Conditional requirements are declarative.** `extraction.discriminator` names an enum field
  and `extraction.required_by_discriminator` lists the fields each variant must supply. This is
  what lets one strict schema serve both `individual` and `cohort` cases without the model being
  forced to invent organizations for a cohort finding. It is generic — any topic can pick its
  own discriminator.
- **`articles` stays global**, keyed by `story_id`. Article text is topic-independent, so two
  topics sharing a story share one fetch. Only *decisions* are topic-scoped. See §5.1.

### Resolved — C9–C12

| # | Item | Fix |
|---|---|---|
| C9 | `init_db` raised bare `RuntimeError`; `ConfigError(RuntimeError)` was standalone | Added `errors.py`, re-rooted `ConfigError`, and raise `DatabaseError` for migration failures |
| C10 | `CREATE TABLE IF NOT EXISTS` inside a versioned migration could hide drift | Migrations use plain `CREATE TABLE`; only indexes retain `IF NOT EXISTS` |
| C11 | `_load_required_env` re-parsed `.env` for each required LLM value | Each credential loader parses its env file once |
| C12 | `cli.main()` was dead code because the entry point targeted `cli:app` | Entry point now targets `cli:main` |

### Migration waiver — expired

**R-DB-1** normally forbids editing a released migration. It was waived once for C1–C3, after
confirming no `data/mc.db` existed; `_migration_1` was amended in place and `SCHEMA_VERSION`
stays `1`. **That waiver is now spent.** All further schema changes are additive: add
`_migration_2` and bump `SCHEMA_VERSION`.

Anyone holding a database created before 2026-07-27 must delete and re-run `init-db`; it
predates the C1–C3 columns and no upgrade path exists.


---

## 3. Invariants

These outrank convenience, elegance, and throughput.

- **INV-1 Quota safety.** A completed search window is never re-queried for the same semantic
  fingerprint.
- **INV-2 Provenance.** Raw API payloads, query parameters, package versions, prompt/schema
  hashes, fetch attempts, and case evidence stay auditable.
- **INV-3 Idempotence.** Every stage resumes incomplete work and is safe to rerun.
- **INV-4 No silent loss.** Duplicates, QC rejects, fetch failures, irrelevant articles, and
  invalid extractions stay represented in the database. Never delete a record to make a rerun
  succeed or to improve reported coverage.
- **INV-5 Secret safety.** Credentials stay in ignored `config/.env` and never reach logs, Git,
  SQLite, exports, or test fixtures.

---

## 4. Verified baseline

Verified 2026-07-27 against `mediacloud==5.1.0` with bounded live calls. Full evidence and
source URLs: `docs/api-notes.md`.

- Client requires Python ≥ 3.10; development pinned to 3.12 via uv.
- Search **requires** at least one collection or source ID (otherwise HTTP 422:
  `Must have at least one of 'ss' or 'cs' parameters`).
- `story_list(...) -> (list[Story], pagination_token)`, supports `page_size` and `randomized`;
  the server-side `page_size` maximum is undocumented, so it stays configurable.
- Standard results for this account are **metadata only**, exactly eight keys:
  `id`, `indexed_date`, `language`, `media_name`, `media_url`, `publish_date`, `title`, `url`.
- The `Story` TypedDict permits optional `text`, but `expanded=True` returns **HTTP 403** for
  this account. Full-text acquisition (Stage 3) is therefore required, and Stage 3 must still
  honour `mc_text` if a future response supplies it.
- Search is not title-only: live results matched without query terms in their titles.
- `LLM_BASE_URL` already ends in `/v1` and must be passed unchanged.

---

## 5. Data model

SQLite with WAL, foreign keys, `synchronous=NORMAL`, `busy_timeout=5000`, UTC ISO-8601 text
timestamps, and `PRAGMA user_version` migrations.

### 5.1 Scoping rule — read this before adding any table

| Scope | Meaning | Tables |
|---|---|---|
| **Global** | Facts about the story or its HTML that no topic can change | `stories`, `articles`, `fetch_attempts` |
| **Topic-scoped** | Decisions derived from topic config | `story_topic_state`, `search_windows`, `search_hits` (via window), `extraction_batches`, `story_extractions`, `cases` |

`articles` being global and keyed by `story_id` is **deliberate and correct** — article text is
topic-independent, so two topics sharing a story share one fetch. Do not "fix" it. Stage 3
selects its work by joining `search_hits → search_windows.topic`.

Anything computed from `TopicConfig` is topic-scoped. This is the rule C1 and C2 violate.

### 5.2 Tables

- **`pipeline_runs`** — one row per stage execution: UUID, topic, stage, status, config
  hash/JSON, Git revision, package versions, timestamps, bounded error detail.
- **`search_windows`** — quota cache and request provenance: exact query, semantic hash,
  request fingerprint, platform, collection/source/language JSON, date bounds, counts, page
  count, `pagination_completed`, client version, status, raw response metadata, error.
  A window becomes complete **only** after the final pagination token is exhausted.
- **`stories`** — one global row per Media Cloud story: normalized searchable metadata,
  optional `mc_text`, `raw_json`, first/last-seen. **No QC or dedup columns** (see C1).
- **`search_hits`** — many-to-many story ↔ window, preserving page number, result rank,
  discovery timestamp, per-hit raw JSON. This is what makes a story reachable from a topic.
- **`story_topic_state`** — *(new, C1)* per-topic QC and dedup decisions.
- **`articles`** — current terminal fetch/extraction state per story: selected source, final
  URL, text, metadata, raw HTML path, error.
- **`fetch_attempts`** — append-only: request URL, source, retry number, HTTP status, elapsed
  time, `Retry-After`, extractor, text length, error, raw metadata.
- **`extraction_batches`** — model/base-URL identifier, prompt/schema hashes, prompt version,
  story IDs, attempt count, request/response/usage JSON, status, timestamps.
- **`story_extractions`** — one relevance decision per (topic, story, prompt version), plus
  validation outcome.
- **`cases`** — one or more findings per relevant article; supports `individual` (named person
  and transition fields) and `cohort` (nullable person, cohort name/size/period, group claim),
  with shared audit fields.

Patronage-only fields from `gold_cases.csv` (party, donation values) are outside this topic's
output contract.

### 5.3 Topic-scoping in the live schema (C1–C3, applied)

`src/mc_pipeline/db.py` is authoritative; this is the shape to preserve.

```sql
-- C1: verdicts that depend on topic config live outside the global stories row
CREATE TABLE story_topic_state (
    topic           TEXT NOT NULL,
    story_id        TEXT NOT NULL REFERENCES stories(story_id) ON DELETE CASCADE,
    qc_status       TEXT,           -- ok|dup|bad_lang|off_domain|bad_url|short_title|out_of_range
    qc_reason       TEXT,
    dup_of_story_id TEXT REFERENCES stories(story_id) ON DELETE SET NULL,
    decided_at      TEXT,
    PRIMARY KEY (topic, story_id)
);

-- C2 + C3: topic-scoped extractions that can record a rejected payload
CREATE TABLE story_extractions (
    ...
    topic             TEXT NOT NULL,
    validation_status TEXT CHECK (validation_status IN ('valid', 'invalid', 'unparsed')),
    validation_error  TEXT,
    UNIQUE(topic, story_id, prompt_version)
);
```

Any new table storing a decision derived from `TopicConfig` must be keyed by topic too.

### 5.4 Transaction protocol

- **R-DB-1** Never edit a released migration once any database may hold data. The one-time
  waiver for C1–C3 is **spent** (§2); all further changes are additive.
- **R-DB-2** One migration version = forward-only, deterministic schema changes.
- **R-DB-3** Apply migrations before stage work.
- **R-DB-4** One transaction per search page, fetch result, extraction batch, or export snapshot.
- **R-DB-5** Never hold a transaction open across a network call or a rate-limit sleep.
- **R-DB-6** Persist raw JSON and normalized columns in the same transaction.
- **R-DB-7** Use `INSERT … ON CONFLICT DO UPDATE` deliberately; never `INSERT OR REPLACE` where
  it could delete and recreate a referenced row.
- **R-DB-8** Booleans as `0/1`; dates and timestamps as ISO-8601 UTC text.
- **R-DB-9** Parameterized SQL only. Repository functions in `db.py`, not inline SQL in `cli.py`.

Intended additions to `db.py`:

```python
@contextmanager
def transaction(connection: sqlite3.Connection) -> Iterator[sqlite3.Connection]: ...
def start_pipeline_run(...) -> int: ...
def complete_pipeline_run(..., status: str, error: str | None = None) -> None: ...
```

---

## 6. Stage contracts

Each stage owns its rules, service signature, done-checklist, and tests. Shared shape:

```python
@dataclass(frozen=True)
class StageSummary:
    topic: str
    processed: int
    succeeded: int
    skipped: int
    failed: int
```

**Per-record failures are data; stage infrastructure failures are command failures.**
`StageSummary.failed > 0` alone does not produce a nonzero exit when the failures are an
expected persisted outcome and the command finished its work.

**Module ownership** — each external system has exactly one owning module; others use its
public interface.

| Module | Owns | Must not own |
|---|---|---|
| `config.py` | YAML models, env-name resolution, stage credentials | API calls, DB writes |
| `errors.py` | `PipelineError` hierarchy | anything else |
| `identity.py` | canonical JSON, SHA-256 hashes, window fingerprints, case IDs | config parsing, I/O |
| `contracts.py` | config → JSON Schema, validation model, CSV columns | provider calls, DB writes |
| `db.py` | connections, migrations, transactions, repositories | stage orchestration |
| `ratelimit.py` | clocks, token bucket, per-domain delay, `Retry-After` parsing | HTTP clients |
| `search.py` | **all** `mediacloud` imports/calls, windowing, cache writes | article HTTP |
| `dedup.py` | deterministic normalization, duplicate and QC decisions | network calls |
| `fetch.py` | robots, live HTTP, Wayback, HTML storage, text extraction | Media Cloud search |
| `llm.py` | OpenAI-compatible client, capability detection, paced requests | prompt assembly |
| `extract.py` | schema/prompt building, packing, validation, case persistence | HTTP client setup |
| `export.py` | deterministic CSV queries/writes, reconciliation | extraction decisions |
| `cli.py` | argument parsing, stage invocation, user-facing summaries | stage business logic |

Importing `mediacloud.api` outside `search.py`, or constructing an OpenAI client outside
`llm.py`, is prohibited. `scripts/probe_api.py` is the only diagnostic exception.

---

### 6.1 Stage 1 — Search

```python
def estimate_topic(
    config: AppConfig, topic_name: str, connection: sqlite3.Connection, *, dry_run: bool = False
) -> StageSummary: ...
def search_topic(
    config: AppConfig,
    topic_name: str,
    connection: sqlite3.Connection,
    *,
    limit_windows: int | None = None,
    dry_run: bool = False,
) -> StageSummary: ...
```

**In:** `TopicConfig` · **Out:** `search_windows`, `stories`, `search_hits`

- **S1-R1** Validate collection/source scope **before** constructing `SearchApi` (verified 422).
- **S1-R2** Default study collection is `34411583`; never substitute another silently.
- **S1-R3** Split the range into closed, non-overlapping windows of `window_days`.
- **S1-R4** `estimate` calls `story_count` only, never `story_list`.
- **S1-R5** Skip windows whose fingerprint is already complete (**INV-1**).
- **S1-R6** Use `identity.window_fingerprint()`. It covers query, platform, collection IDs,
  source IDs, languages, window bounds, and contract version — and deliberately has no
  request-control parameter, so `page_size` cannot invalidate a completed window (C7).
- **S1-R7** Pass `datetime.date` at the client boundary; `expanded=False` — do not retry the
  known 403.
- **S1-R8** Persist each page before requesting the next; mark the window complete only after
  the token is exhausted. Never mark complete after a partial page failure.
- **S1-R9** On reaching `max_pages_per_window`, persist the window incomplete and return an
  infrastructure failure for operator review.
- **S1-R10** Retry only 429, connection failures, and bounded 5xx. Honour `Retry-After`;
  otherwise exponential backoff with jitter and a maximum delay.
- **S1-R11** Pagination tokens are opaque transient progress — never a cache key.

**Done when:** missing scope fails before any API call · the eight-key payload round-trips
through `raw_json` · one story can belong to multiple windows · a completed window produces
**zero** API calls on rerun · incomplete pagination is never marked complete.

**Live progression (do not skip):** directory lookup or `estimate` → one short-window search
with an explicit window limit → second invocation proving zero API calls → expand only after
reconciliation **and** user approval.

---

### 6.2 Stage 2 — Deduplicate and QC

```python
def deduplicate_topic(
    config: AppConfig, topic_name: str, connection: sqlite3.Connection
) -> StageSummary: ...
```

**In:** `stories` reachable from the topic via `search_hits` · **Out:** `story_topic_state`

Pure function of the database — no network, no credentials, free to rerun after tuning rules.

- **S2-R1** Write decisions to `story_topic_state`, keyed by `(topic, story_id)`, never to
  `stories` (C1). Two topics must be able to disagree about the same story.
- **S2-R2** Normalize URLs: drop scheme and `www.`, tracking params (`utm_*`, `fbclid`),
  fragments, trailing slash.
- **S2-R3** Normalize titles: case-fold, collapse punctuation and whitespace, strip outlet
  suffixes (`" | CBC News"`, `" - The Globe and Mail"`, `" — National Post"`).
- **S2-R4** Order: URL duplicates → exact-title duplicates → optional near-duplicates within a
  small publication-date block (`rapidfuzz.token_set_ratio ≥ near_dup_threshold`, blocked by
  `publish_date ± 3 days` to stay O(n·k)).
- **S2-R5** Canonical row for a title-duplicate group is the **earliest** `publish_date`.
- **S2-R6** Apply language, date-range, URL-pattern, title-length, and optional domain rules.
  An empty `domain_allowlist` means no extra domain filter — the collection already scopes sources.
- **S2-R7** Never delete a rejected story (**INV-4**). Only `qc_status='ok' AND
  dup_of_story_id IS NULL` proceeds to Stage 3.

**Done when:** deterministic on fixtures · idempotent on rerun · two topics can hold different
`qc_status` for the same story (the C1 regression test).

---

### 6.3 Stage 3 — Acquire full text

```python
def fetch_topic(
    config: AppConfig,
    topic_name: str,
    connection: sqlite3.Connection,
    *,
    limit: int | None = None,
    retry_failed: bool = False,
    dry_run: bool = False,
) -> StageSummary: ...
```

**In:** QC-passing stories · **Out:** `articles`, `fetch_attempts`, gzipped raw HTML

Order of acquisition:

1. Use `stories.mc_text` when a future response supplies it and it meets `min_text_chars`
   (`source='mediacloud'`) — no HTTP.
2. Live URL with robots check, global and per-domain limits, bounded retries, research user agent.
3. Extract from **pre-fetched** HTML: `trafilatura` → `readability-lxml` → BeautifulSoup
   paragraph fallback. First result ≥ `min_text_chars` wins; record which extractor succeeded.
4. On block, failure, or short text: Wayback availability
   (`https://archive.org/wayback/available?url=…&timestamp=YYYYMMDD`) then the original-byte
   replay `https://web.archive.org/web/<timestamp>id_/<url>`; re-run the extractor chain.
5. Record a terminal state always: `ok` | `fetch_failed` | `too_short` | `skipped`.

- **S3-R1** Extractors never fetch URLs; the pipeline owns every request.
- **S3-R2** Check cached robots rules before a domain's first request.
- **S3-R3** Apply global and per-domain limits before every live *and* Wayback request; Wayback
  gets its own throttle entry.
- **S3-R4** Configure a real contact address in `user_agent` before production crawling.
- **S3-R5** Retry safe methods only. Cap redirects, response bytes, retry count, total time.
- **S3-R6** Accept HTML-like content types only; record anything else.
- **S3-R7** Save raw HTML atomically (temp file then rename); derive paths from safe
  topic/story identifiers, never from untrusted URL path strings.
- **S3-R8** Re-extraction from stored HTML must make **no** network calls.
- **S3-R9** Wayback is a fallback for failure, not a means to bypass access controls. **Do not
  implement paywall bypass.**
- **S3-R10** `too_short` stays distinct from transport `fetch_failed`.

**Done when:** mocked success, retry, permanent failure, and Wayback-fallback paths are all
covered · one domain's delay is observably respected under a fake clock · a rerun re-attempts
only `fetch_failed` rows.

---

### 6.4 Stage 4 — Extract cases

```python
def extract_topic(
    config: AppConfig,
    topic_name: str,
    connection: sqlite3.Connection,
    *,
    limit: int | None = None,
    articles_per_request: int | None = None,
    dry_run: bool = False,
) -> StageSummary: ...
```

**In:** `articles` with `fetch_status='ok'` · **Out:** `extraction_batches`,
`story_extractions`, `cases`

- **S4-R1** Operate only on stored text with terminal fetch status `ok`. Never fetch a URL here.
- **S4-R2** Build the schema with `contracts.build_response_json_schema()` and the prompt
  deterministically from topic config; hash the exact system prompt, user-template version,
  and schema.
- **S4-R3** Client with `max_retries=0` — pipeline pacing and retry are authoritative.
- **S4-R4** Serial requests, ≤ 30 per minute, via the shared token bucket.
- **S4-R5** Pack `articles_per_request` (default 5) labelled articles; delimit each with an
  immutable story ID; validate returned IDs against the batch.
- **S4-R6** Model-provided IDs, URLs, or evidence must never update an unrelated story.
- **S4-R7** Try strict JSON Schema; on an **explicit unsupported-format** response fall back to
  JSON-object mode with the schema inlined, and record the downgrade once. Do not downgrade on
  unrelated 400s.
- **S4-R8** Validate every response with `contracts.build_response_model()` even when the
  provider claims strict conformance.
- **S4-R9** Config `required` drives **Pydantic**, not the schema `required` array (C6).
  Conditional rules come from `extraction.discriminator` and
  `extraction.required_by_discriminator` — never hardcode `case_type` semantics in Python.
- **S4-R10** Generate `case_id` with `identity.build_case_id()` (C4); never from the model.
- **S4-R11** Invalid or unparsable output ⇒ `story_extractions.validation_status` set,
  payload retained, **zero** `cases` rows. Never let an `IntegrityError` abort a batch (C3).
- **S4-R12** Normalize whitespace for evidence matching only; store the original quote.
  Unverified evidence is flagged explicitly, not dropped.
- **S4-R13** On missing story IDs, requeue only the missing items. On context overflow, split
  the batch deterministically — do not truncate harder.
- **S4-R14** Record provider request IDs and usage; never headers or secrets.
- **S4-R15** Unknown proxy model names fall back to `tiktoken.get_encoding("o200k_base")` for
  token estimates.

**Done when:** `GC008`-shaped individual and `GC011`-shaped cohort records both validate · one
article can produce multiple cases · an irrelevant article is persisted with a reason · a
schema-violating response is persisted as `invalid` without aborting the batch · evidence
verification status is explicit.

---

### 6.5 Stage 5 — Export

```python
def export_topic(
    config: AppConfig,
    topic_name: str,
    connection: sqlite3.Connection,
    *,
    output_dir: Path = Path("data/out"),
) -> StageSummary: ...
```

Two UTF-8 RFC-4180 CSVs:

- `<topic>.csv` — one row per case
- `<topic>_articles.csv` — one row per story, showing dedup/QC/fetch/relevance coverage

**Column rule (C5, implemented):** call `contracts.case_csv_columns(topic)` — the prefix,
config fields in order, and suffix are composed there and nowhere else.

```text
# PROVENANCE_PREFIX (pipeline-derived)
topic,case_id,story_id,url,domain,publish_date,media_name,title,
source_outlet,source_url,link_category,
# … extraction.fields in config order …
# AUDIT_SUFFIX
evidence_verified,model,prompt_version,extracted_at
```

`source_outlet` = `media_name`, `source_url` = `url`, `link_category` = the topic constant.
Article-audit CSV columns come from `contracts.ARTICLE_AUDIT_COLUMNS`:

```text
topic,story_id,url,domain,publish_date,media_name,title,qc_status,qc_reason,
dup_of_story_id,fetch_status,source,text_chars,relevant,reject_reason
```

- **S5-R1** One header row; take the column order from `contracts` — never write a second list.
- **S5-R2** Empty string for nullable values; never literal `None` or `null`.
- **S5-R3** Preserve claims and evidence with correct CSV quoting.
- **S5-R4** Write to a temp file and atomically replace.
- **S5-R5** Sort by `publish_date`, `story_id`, `case_id`.
- **S5-R6** Reconcile row counts before replacement; a mismatch is exit code 7.
- **S5-R7** `tests/test_contracts.py` already guards config↔`cases`↔CSV agreement and rejects
  a field name that collides with a reserved column. Changing CSV columns silently is prohibited.

**Reconciliation identity:** `hits = duplicates + qc_rejects + fetched_ok + fetch_failed + too_short`.

---

## 7. Cross-cutting protocols

### 7.1 Environment (R-ENV)

```bash
UV=/opt/homebrew/bin/uv
export UV_CACHE_DIR=/tmp/media-cloud-uv-cache
$UV sync --python 3.12
```

- **R-ENV-1** Never system Python 3.9 — `mediacloud==5.1.0` needs ≥ 3.10.
- **R-ENV-2** Never `pip install`. Edit `pyproject.toml`, then `uv sync`.
- **R-ENV-3** Every Python command, script, test, lint, and type check runs under `uv run`.
- **R-ENV-4** Commit `uv.lock` whenever resolution changes.
- **R-ENV-5** Language target is 3.11 (`ruff target-version`, `mypy python_version`);
  verification interpreter is 3.12. This split is deliberate — do not "align" them.

### 7.2 Secrets (R-SEC)

- **R-SEC-1** Read secrets only through `load_media_cloud_token()` / `load_llm_credentials()`.
- **R-SEC-2** Never open, print, echo, serialize, snapshot, fixture, or log values from `config/.env`.
- **R-SEC-3** Hold `SecretStr`; call `get_secret_value()` only at client construction.
- **R-SEC-4** Redact authorization headers, query-string credentials, and provider error bodies.
- **R-SEC-5** Store the LLM base URL as provenance; never the API key. Use `LLM_BASE_URL`
  exactly — do not append `/v1`.
- **R-SEC-6** Tests use temporary fake secrets and never read the real `config/.env`.
- **R-SEC-7** When diagnosing config, inspect variable **names** and ignore status only.

### 7.3 Errors (R-ERR)

`src/mc_pipeline/errors.py` — create this before the next stage (C9):

```text
PipelineError
├── ConfigError        (re-root the existing one here)
├── DatabaseError
├── MediaCloudError
├── FetchError
├── ExtractionError
└── ExportError
```

Classification:

- `permanent_record` — invalid URL, robots denial, unsupported content, irrelevant article
- `retryable_record` — transient HTTP/Wayback/provider failure below the retry limit
- `infrastructure` — authentication, schema/migration failure, repeated provider outage,
  filesystem failure, invalid capability negotiation

- **R-ERR-1** Persist record-level outcomes; abort only on infrastructure errors, after closing
  the open transaction and marking the pipeline run failed.
- **R-ERR-2** Do not catch bare `Exception` in production paths unless immediately re-raising a
  typed `PipelineError` with preserved `__cause__` and bounded context.
- **R-ERR-3** Never encode article-level HTTP failures or irrelevant stories as process exit codes.

### 7.4 Git (R-GIT)

- **R-GIT-1** Work on `main` unless the user asks for a branch.
- **R-GIT-2** Inspect `git status --short --branch` before and after changes.
- **R-GIT-3** Treat unrecognized modifications as user work; do not revert them.
- **R-GIT-4** Never `git reset --hard`, destructive checkout/clean, or history rewriting.
- **R-GIT-5** Never commit, push, add a remote, or open a PR unless explicitly asked.
- **R-GIT-6** Before handoff: `git check-ignore -v config/.env` must confirm the file is ignored.
- **R-GIT-7** Never stage or display credential contents.

### 7.5 Coding (R-CODE)

- **R-CODE-1** Type annotations on public functions; strict-mypy-compatible.
- **R-CODE-2** Pydantic for external/configured data; dataclasses or `TypedDict` for internal transport.
- **R-CODE-3** `pathlib.Path`, timezone-aware UTC, parameterized SQL, context-managed resources.
- **R-CODE-4** Canonical JSON (sorted keys, compact separators) + SHA-256 for every hash and
  fingerprint.
- **R-CODE-5** Clients, clocks, sleepers, and HTTP sessions are injectable for tests.
- **R-CODE-6** Public functions small enough to test without network access.
- **R-CODE-7** Comments explain non-obvious invariants or provider behaviour, not syntax.
- **R-CODE-8** Stage modules depend on the service signatures in §6, not on each other's internals.
- **R-CODE-9** No unrelated refactoring while implementing a stage.

```bash
$UV run ruff format . && $UV run ruff check . && $UV run mypy src
```

### 7.6 Dependencies (R-DEP)

- **R-DEP-1** Prefer the standard library or an existing dependency.
- **R-DEP-2** Add one only when it materially simplifies a domain problem; justify in `pyproject.toml`.
- **R-DEP-3** Add fetch/extract/export dependencies when those stages are implemented, so the
  lock file reflects real code. Expected later additions: `requests`, `trafilatura`,
  `readability-lxml`, `beautifulsoup4`, `lxml`, `tenacity`, `rapidfuzz`, `openai`, `tiktoken`.

### 7.7 Rate limiting (`ratelimit.py`)

- `TokenBucket(rate_per_min)` — shared by the global fetch cap and the LLM's 30 rpm.
- `PerDomainThrottle(delay_s)` — `domain → last_request_monotonic`; sleeps the remainder.
- `parse_retry_after(header)` — handles both integer-seconds and HTTP-date forms.
- `time.monotonic()` throughout; the clock and sleeper are injectable (**R-CODE-5**).

### 7.8 Testing (R-TEST)

Layers: **unit** (no network, temp dirs, fake clock) → **adapter** (mocked Media Cloud / HTTP /
LLM with recorded redacted fixtures) → **integration** (bounded live, only when requested) →
**manual accuracy** (human review).

- **R-TEST-1** Every change adds or updates the narrowest relevant tests, in the same change.
- **R-TEST-2** No real network in unit tests; no real sleeping.
- **R-TEST-3** No dependence on row order unless the query orders explicitly.
- **R-TEST-4** Fixtures exclude full copyrighted article bodies — author test text or store a
  short compliant excerpt.
- **R-TEST-5** Database tests use temp paths and close connections.
- **R-TEST-6** CLI tests assert exit code, bounded output, and absence of secret values.
- **R-TEST-7** Tests never read the real `config/.env`.

Required gate before handoff:

```bash
$UV run pytest
$UV run ruff check .
$UV run ruff format --check .
$UV run mypy src
```

### 7.9 Run classes (R-RUN) — never escalate implicitly

| Class | Scope | Preconditions |
|---|---|---|
| **Offline** | `uv sync`, `init-db` on a temp path, `pytest` | none |
| **Diagnostic probe** | `scripts/probe_api.py`, one operation per invocation, `page_size ≤ 10`, bounded metadata only | none |
| **Smoke** | explicit topic, one short window, fetch `--limit ≤ 20`, extract `--limit ≤ 10` | offline gate passes |
| **Full** | complete study range | smoke passed, cache reuse verified, fetch-domain review done, manual accuracy review done, **user approval** |

A full run must write a `pipeline_runs` record with config hash, Git revision, dependency
versions, and timestamps.

### 7.10 Provenance (R-PROV)

Every stage run records topic, stage, canonical config hash and relevant config JSON, Git
revision (or `uncommitted`), Python and package versions, start/completion timestamps, status,
bounded error detail.

Every external request records provider/endpoint **category** (never authorization data),
relevant IDs and date bounds, pagination or attempt number, response status, provider request
ID where available, elapsed time, and normalized response JSON where licensing and size permit.

Raw article HTML is stored as a file reference, never duplicated into request logs. LLM prompts
and responses are stored with prompt/schema hashes and prompt version.

---

## 8. Reference

### 8.1 CLI

```text
mc-pipeline init-db         --db data/mc.db
mc-pipeline resolve-sources --name Canada
mc-pipeline estimate        --topic revolving_door_ca
mc-pipeline search          --topic revolving_door_ca [--limit-windows N]
mc-pipeline dedup           --topic revolving_door_ca
mc-pipeline fetch           --topic revolving_door_ca [--limit N] [--retry-failed]
mc-pipeline extract         --topic revolving_door_ca [--limit N] [--articles-per-request K]
mc-pipeline export          --topic revolving_door_ca
mc-pipeline status          --topic revolving_door_ca
mc-pipeline run             --topic revolving_door_ca
```

Common options: `--config` (default `config/topics.yaml`), `--env-file` (default `config/.env`),
`--db` (default `data/mc.db`), `--verbose/-v` (repeatable), `--dry-run`.

`--dry-run` prints request parameters or a prompt summary and is offered **only** where it can
guarantee no quota, paid, or network mutation.

| Command | May touch | Must not touch |
|---|---|---|
| `init-db` | database | network |
| `resolve-sources` | one Directory API call, bounded results | search |
| `estimate` | `story_count` | `story_list` |
| `search` | Media Cloud | article URLs |
| `dedup` | database | network, credentials |
| `fetch` | article + Wayback HTTP | LLM |
| `extract` | LLM over stored text | article URLs |
| `export` | database, filesystem | network, credentials |
| `status` | database (read-only) | everything else |
| `run` | stages in order, stops on infrastructure failure | — |

### 8.2 Exit codes

```text
0  completed, including persisted per-record terminal failures
2  invalid CLI arguments or configuration
3  missing credentials
4  database or migration failure
5  Media Cloud or article-fetch infrastructure failure
6  LLM infrastructure or capability failure
7  export or reconciliation failure
```

### 8.3 Public interfaces already implemented

```python
# config.py
load_config(path: str | Path = "config/topics.yaml") -> AppConfig
load_media_cloud_token(config: AppConfig, env_file: str | Path = "config/.env") -> SecretStr
load_llm_credentials(config: AppConfig, env_file: str | Path = "config/.env") -> LLMCredentials

# db.py
connect_db(path: str | Path) -> sqlite3.Connection
init_db(path: str | Path) -> sqlite3.Connection
```

Stages receive validated config models, never raw YAML dicts. A command loads only the
credentials its stage needs. Config validation fails before opening the database or making a
network call.

### 8.4 Prohibited actions

- expose or commit credentials
- full-range Media Cloud search without explicit approval
- crawl without configured identification, robots handling, and limits
- bypass paywalls or access controls
- retry permanent 4xx other than handled 408/409/425/429 semantics
- call expanded Media Cloud stories after the verified 403, absent a documented capability change
- send articles to the LLM before validating fetch status and text length
- overwrite completed cache records without a changed fingerprint or contract version
- delete failed or rejected records to improve reported coverage
- change CSV columns silently
- mutate released migrations (waiver in §2 only)
- real network calls in unit tests
- commit, push, or rewrite history without instruction

### 8.5 Accuracy gate before scale

Hand-label ~20 articles; compare precision/recall for person, organizations, direction, case
relevance, and cohort handling. Tune query, `relevance_criteria`, and packing size **before**
running the full 2021–2025 range.

---

## 9. Agent workflow and handoff

1. Read §0, §3, and the stage section you are implementing. Run `git status --short --branch`.
2. Read the target modules, adjacent tests, config models, and relevant schema.
3. State a short implementation plan for multi-file work.
4. Implement the smallest complete vertical change.
5. Add deterministic tests **before** any live validation.
6. Run the §7.8 gate.
7. Run bounded live validation only when requested or essential, and only within its run class (§7.9).
8. Re-read the diff for secrets, unrelated edits, schema drift, and stale docs.
9. Update this file's §0 status table, and `docs/api-notes.md` when external behaviour changes.
10. Hand off in this format:

```text
Outcome:    what now works
Files:      paths and main responsibilities
Validation: exact commands and pass/fail counts
Network:    calls made, limits, whether quota or LLM usage occurred
Data:       database/output paths created or modified
Git:        branch, whether a commit was created
Remaining:  unresolved risks, next stage
```

**Never report "done" when the gate was not run.** State which check could not run and what is
therefore unverified.

### Definition of done (any stage)

- public service signature (§6) and CLI command implemented
- terminal states and idempotent rerun behaviour defined
- writes preserve raw and normalized provenance
- unit and adapter tests cover success, retry, permanent failure, and resume
- §7.8 gate passes
- bounded smoke validation passes when the stage uses an external service
- README, this plan, and `docs/api-notes.md` match actual behaviour
- secret, quota, licensing, and research-audit requirements still hold
