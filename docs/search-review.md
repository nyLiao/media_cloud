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
Stage 2 and later reconciliation use distinct reachable stories, so repeated hits across windows
are counted once before QC, deduplication, fetch, and extraction outcomes are summed.

## Manual Checklist

Open `data/review/revolving_door_ca-search.csv` in a spreadsheet and check:

1. Filter to `window_start=2021-01-01` and `window_end=2025-12-31`. Confirm this configured
   full-study partition is present; older 30-day recovery windows may remain in the same database.
2. Within that full-study partition, confirm `pagination_completed=1`, eight page numbers, and
   7,351 distinct story IDs. Legacy pending windows do not invalidate the completed partition.
3. Filter blank `story_id`, `title`, `url`, or `publish_date` values. Missing provider metadata
   is retained for audit, but should be counted before Stage 2.
4. Sort or filter `domain` and `media_name` to identify unexpected outlets or obvious scope
   problems. The configured collection remains the authoritative source scope.
5. Search `title` for obviously irrelevant themes and note recurring false-positive patterns.
   Do not delete rows; later QC decisions belong in `story_topic_state`.
6. Compare repeated `story_id` values across windows. The current database intentionally retains
   the earlier recovery windows as well as the full-study partition, so cross-window repetition is
   provenance rather than a Stage 1 error; Stage 2 operates on distinct stories.
7. Open a small sample of `url` values to judge query precision. Stage 1 stores metadata only,
   so article-text relevance cannot be fully assessed until Stage 3 fetches text.

Useful read-only reconciliation queries:

```bash
sqlite3 -header -column data/mc.db \
  "SELECT status, COUNT(*) AS windows, SUM(pages_fetched) AS pages
   FROM search_windows WHERE topic='revolving_door_ca' GROUP BY status;"

sqlite3 -header -column data/mc.db \
  "SELECT COUNT(*) AS hits, COUNT(DISTINCT h.story_id) AS distinct_stories,
          COUNT(DISTINCT h.page_number) AS pages
   FROM search_hits h JOIN search_windows w ON w.id=h.search_window_id
   WHERE w.topic='revolving_door_ca'
     AND w.window_start='2021-01-01' AND w.window_end='2025-12-31';"

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
