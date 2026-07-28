# API Notes

Verified on **2026-07-27** against `mediacloud==5.1.0`, the public package/source, and
bounded live calls using the configured Media Cloud account. Credential values were never
printed or copied into these notes.

## Media Cloud Client

### Package and runtime

`CONFIRMED`

- PyPI release: `mediacloud==5.1.0`, released 2026-05-19.
- Package requirement: Python 3.10 or newer.
- Project development interpreter: Python 3.12 through uv.
- Imports: `from mediacloud.api import SearchApi, DirectoryApi`.

Sources:

- https://pypi.org/project/mediacloud/
- https://github.com/mediacloud/api-client

### `story_list` interface

`CONFIRMED` by installed-source introspection.

```python
story_list(
    query,
    start_date,
    end_date,
    collection_ids=[],
    source_ids=[],
    platform=None,
    expanded=False,
    pagination_token=None,
    sort_order=None,
    page_size=None,
    randomized=False,
) -> tuple[list[Story], str | None]
```

The earlier plan was wrong to mark `page_size` as unsupported. It is accepted by the
5.1.0 client and was accepted in the live request. The server-side maximum remains
undocumented, so it stays configurable.

### Required source scope

`CONFIRMED` by live calls.

Search requests must contain at least one source ID or collection ID. Calls without either
returned HTTP 422 with: `Must have at least one of 'ss' or 'cs' parameters`.

The Directory lookup returned:

```text
34411583  Canada - National  online_news
```

This ID is configured in `config/topics.yaml`.

### Standard story shape

`CONFIRMED` by a live `story_list` request with `page_size=3`, collection `34411583`, and
the configured revolving-door query over 2021-01-01 through 2025-12-31.

Every returned story had exactly these keys:

```text
id
indexed_date
language
media_name
media_url
publish_date
title
url
```

Observed Python types:

- `id`, `language`, `media_name`, `media_url`, `title`, `url`: `str`
- `publish_date`: `datetime.date`
- `indexed_date`: `datetime.datetime`

The response included a pagination token. The returned titles did not necessarily contain
the query terms, which confirms the search is not title-only for this query/index.

Malformed records in an otherwise successful page do not fail the window. Valid records are
persisted, while page metadata records each rejected result rank, error, and serializable raw
payload. Invalid URL syntax is retained with a null domain and becomes `bad_url` during Stage 2 QC.

### Full page text

`CONFIRMED` for this account.

The package's `Story` `TypedDict` permits an optional `text: str` field, which explains the
full-text capability implied by the package model. However:

- standard live results contained no `text` field
- `expanded=True` returned HTTP 403: the account is not permitted to fetch expanded stories

Therefore the pipeline must preserve optional `text` if a future response supplies it, but
must use direct live-page extraction for the current account and skip failures by default.

### 2026-07-27 quota and text audit

The first production attempt demonstrated that fixed 30-day windows were too expensive for
this study. Sixty-one count calls were made before search, then 47 result-page calls completed
through `2024-11-10`. A later full-range search completed in eight result pages and brought the
database to 7,351 distinct metadata stories, equal to the sum of the earlier window estimates,
with zero populated `mc_text` values. Windowing is a pipeline recovery choice, not a Media Cloud
API requirement.

The default is now one full-range partition with `page_size=1000`, no estimate prerequisite,
and no automatic retries. Smaller date partitions are reserved for explicit recovery only.

A bounded availability check of eight stored URLs returned six HTTP 200 responses, one 403,
and one 404. Direct article-page fetching is therefore applicable to many current records, but
blocked or missing pages should be skipped after one attempt. Media Cloud expanded text remains
unavailable for this account.

### Counts and pagination

`CONFIRMED` from 5.1.0 installed types/source; not re-probed to conserve quota.

- `story_count(...) -> StoryCount` with optional `relevant: int` and `total: int`.
- `story_count_over_time(...) -> list[CountOverTimePoint]` containing `date`, `count`,
  `total_count`, and `ratio`.
- `story_list` returns `(stories, pagination_token)`.
- Pagination tokens are opaque and must be persisted only as transient progress; a window
  is marked complete only after the final token is exhausted.

### API key convention

`CONFIRMED` as a project convention.

The project reads `MC_API_TOKEN` from `config/.env`. The file is ignored by Git and its
contents must not be logged or stored in SQLite.

## Full-text Acquisition

### Extractors

`CONFIRMED` from current package documentation.

Use pre-fetched HTML with this fallback order:

1. `trafilatura.extract(...)`
2. `readability.Document(html).summary()`
3. BeautifulSoup paragraph extraction

The pipeline owns HTTP requests, robots handling, throttling, retries, and raw-HTML storage;
extractors must not fetch URLs themselves.

Sources:

- https://trafilatura.readthedocs.io/en/latest/corefunctions.html
- https://pypi.org/project/readability-lxml/

### Wayback reference — disabled by default

`CONFIRMED`

- Availability endpoint:
  `https://archive.org/wayback/available?url=<url>&timestamp=YYYYMMDD`
- Closest capture path: `archived_snapshots.closest`
- Original-byte replay:
  `https://web.archive.org/web/<timestamp>id_/<url>`

The endpoints were verified for reference, but the pipeline does not call Wayback by default.
Blocked, missing, or short pages are skipped after the single live request. Do not implement
paywall bypasses.

Source:

- https://archive.org/help/wayback_api.php

### Single-attempt crawl policy

`CONFIRMED`

- Use a `requests.Session` with automatic retries disabled.
- Add a single-threaded per-domain monotonic-clock delay sampled uniformly from one second through
  the configured maximum, plus a separate global request limiter.
- Use `urllib.robotparser.RobotFileParser`; cache rules per domain.
- Record every fetch attempt and terminal failure instead of dropping articles.
- Cap live responses at five redirects and 5 MiB, and accept only HTML/XHTML content types.
- Keep dry runs read-only and network-free; show dynamic progress only for interactive CLI output.

## OpenAI-compatible Proxy

### Base URL and credentials

`CONFIRMED` from local configuration shape.

- Read `LLM_BASE_URL` and `LLM_API_KEY` from `config/.env`.
- The configured base URL ends in `/v1`; pass it unchanged to `OpenAI(base_url=...)`.
- Never persist the API key or authorization headers.

### Structured output

`CONFIRMED` for the OpenAI SDK; proxy capability remains runtime-detected.

1. Try Chat Completions JSON Schema structured output.
2. On an explicit unsupported-format response, fall back to JSON object mode with the
   schema included in the prompt.
3. Validate every response locally with Pydantic.
4. Store prompt/schema hashes and response JSON for reproducibility.

Source:

- https://platform.openai.com/docs/guides/structured-outputs

### Retries and packing

- Construct the client with `max_retries=0`; the pipeline also makes one attempt only.
- Keep requests serial and at or below 30 requests/minute.
- Default to five labeled articles per request and skip missing story IDs rather than requeueing.
- Treat five as an experimental default; measure accuracy against hand-labeled cases before
  scaling.
- Unknown proxy model names fall back to `tiktoken.get_encoding("o200k_base")` for estimates.
- Do not assume the proxy exposes OpenAI's `/v1/batches` endpoint.

## Probe Policy

`scripts/probe_api.py` performs exactly one operation per invocation:

```bash
uv run python scripts/probe_api.py collections --collection-name Canada
uv run python scripts/probe_api.py count --query '...' --collection-id 34411583 \
  --start-date 2021-01-01 --end-date 2021-01-31
uv run python scripts/probe_api.py list --query '...' --collection-id 34411583 \
  --start-date 2021-01-01 --end-date 2021-01-31 --page-size 3
```

List probes are capped at ten stories and print bounded metadata, key sets, and text length,
not article bodies or credentials.
