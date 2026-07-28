# Deduplication and QC Review

`review-dedup` exports the persisted Stage 2 decisions to a deterministic CSV for inspection
before Stage 3 fetches article text. It does not call Media Cloud, read credentials, or change
the SQLite database.

## Workflow

Run Stage 2 yourself, then export its decisions:

```bash
scripts/run_dedup_qc.sh --topic revolving_door_ca
uv run mc-pipeline review-dedup --topic revolving_door_ca
```

The default output path is `data/review/<topic>-dedup.csv`. Select another path with `--output`:

```bash
uv run mc-pipeline review-dedup \
  --topic revolving_door_ca \
  --output data/review/revolving-door-stage-2.csv
```

The command prints reconciliation totals for all distinct stories reachable from the topic's
Stage 1 search windows:

- `accepted` has `qc_status=ok` and is eligible for Stage 3.
- `duplicates` has `qc_status=dup`; `duplicate_urls` and `duplicate_titles` split the matching
  rule used.
- `rejected` failed QC before deduplication, with the reason in `qc_reason`.
- `undecided` is reachable from Stage 1 but has no Stage 2 state, usually because `dedup` has not
  yet run after new search results were stored.

Rows are ordered by `story_id`. Duplicate rows include `dup_of_story_id` and selected metadata
from the canonical story, so they can be inspected without a second database query.

## Manual Checklist

Open `data/review/revolving_door_ca-dedup.csv` in a spreadsheet and check:

1. Confirm `undecided=0` before starting Stage 3. Rerun `dedup` if Stage 1 has added stories.
2. Filter `qc_status=dup` and compare each row with its `dup_of_story_id`; the canonical story
   is chosen deterministically by earliest publish date, then story ID.
3. Review `qc_reason=bad_lang`, `out_of_range`, `bad_url`, `short_title`, and `off_domain` for
   expected scope decisions. Adjust `topics.<name>.qc` or `domain_allowlist` only when the rule,
   rather than an individual outcome, is incorrect.
4. Filter `qc_status=ok` to see the exact population Stage 3 can fetch. Do not edit the CSV;
   rerun Stage 2 after changing configuration so the database preserves provenance.

## Command

`review-dedup` accepts `--topic`, `--config`, `--db`, and `--output`. It is deliberately local
and has no `--env-file` or `--dry-run` option.

## Exit Codes

```text
0  command completed
2  invalid CLI arguments or configuration
4  database or migration failure
5  pipeline infrastructure failure
```
