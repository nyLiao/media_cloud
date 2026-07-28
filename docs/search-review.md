# Search Review

`review-search` exports the persisted output of Stage 1 to a CSV that can be reviewed before
deduplication, article fetching, or extraction. It does not call Media Cloud and does not read
credentials.

## Workflow

Run the single-partition search yourself, then export the resulting review file:

```bash
scripts/run_media_cloud_search.sh --topic revolving_door_ca
uv run mc-pipeline review-search --topic revolving_door_ca
```

The default output path is `data/review/<topic>-search.csv`. Choose a different path with
`--output`:

```bash
uv run mc-pipeline review-search \
  --topic revolving_door_ca \
  --output data/review/revolving-door-first-window.csv
```

The command prints window and hit reconciliation totals, then writes one row per search hit.
The same story can legitimately appear in more than one window; `duplicate_hits` reports this
difference without deleting anything.

## Manual Checklist

Open `data/review/revolving_door_ca-search.csv` in a spreadsheet and check:

1. Sort by `window_start`, `page_number`, and `result_rank`. Confirm the windows cover
   `2021-01-01` through `2025-12-31` without unexpected gaps.
2. Filter `pagination_completed` to `0`. A full successful run must have no matching rows;
   inspect `search_windows.status` in SQLite if any window is incomplete.
3. Filter blank `story_id`, `title`, `url`, or `publish_date` values. Missing provider metadata
   is retained for audit, but should be counted before Stage 2.
4. Sort or filter `domain` and `media_name` to identify unexpected outlets or obvious scope
   problems. The configured collection remains the authoritative source scope.
5. Search `title` for obviously irrelevant themes and note recurring false-positive patterns.
   Do not delete rows; later QC decisions belong in `story_topic_state`.
6. Compare repeated `story_id` values across windows. These are preserved search hits, not a
   Stage 1 error; Stage 2 performs deterministic deduplication.
7. Open a small sample of `url` values to judge query precision. Stage 1 stores metadata only,
   so article-text relevance cannot be fully assessed until Stage 3 fetches text.

Useful read-only reconciliation queries:

```bash
sqlite3 -header -column data/mc.db \
  "SELECT status, COUNT(*) AS windows, SUM(pages_fetched) AS pages
   FROM search_windows WHERE topic='revolving_door_ca' GROUP BY status;"

sqlite3 -header -column data/mc.db \
  "SELECT COUNT(*) AS hits, COUNT(DISTINCT h.story_id) AS distinct_stories
   FROM search_hits h JOIN search_windows w ON w.id=h.search_window_id
   WHERE w.topic='revolving_door_ca';"

sqlite3 -header -column data/mc.db \
  "SELECT window_start, window_end, relevant_count,
          (SELECT COUNT(*) FROM search_hits h WHERE h.search_window_id=w.id) AS stored_hits
   FROM search_windows w WHERE topic='revolving_door_ca'
   ORDER BY window_start;"
```

## Commands

`search` retrieves and persists matching stories. `estimate` remains available for diagnostics,
but is not part of the normal workflow because it spends quota without retrieving story IDs.
Both commands accept these common options:

```text
--topic NAME       configured topic name (required)
--config PATH      configuration YAML (default: config/topics.yaml)
--env-file PATH    credential file (default: config/.env)
--db PATH          SQLite database (default: data/mc.db)
--dry-run          show the stage request without a Media Cloud call
```

The configured study uses one full-range partition. `--limit-windows` is retained only for a
future explicitly partitioned recovery configuration.

`review-search` accepts `--topic`, `--config`, `--db`, and `--output`; it is deliberately local
and has no `--env-file` or `--dry-run` option.

## Exit Codes

```text
0  command completed
2  invalid CLI arguments or configuration
3  missing credentials
4  database or migration failure
5  Media Cloud infrastructure failure
```

Rerunning an identical completed window is safe: its semantic fingerprint is reused and no
Media Cloud request is made. Incomplete windows resume from the persisted opaque pagination
token. If a run stops, rerun the same command rather than deleting database rows.
