"""SQLite connection helpers and schema migrations for the pipeline cache."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from itertools import count
from pathlib import Path
from typing import Any, cast

from .errors import DatabaseError

SCHEMA_VERSION = 3
BUSY_TIMEOUT_MS = 5000
MAX_FETCH_REVIEW_LIMIT = 100
_SAVEPOINT_COUNTER = count()


def connect_db(path: str | Path) -> sqlite3.Connection:
    """Open a configured SQLite connection at *path*."""
    database_path = Path(path)
    if str(path) != ":memory:":
        database_path.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(str(path))
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA synchronous = NORMAL")
    # WAL still serializes writers; without a busy timeout any concurrent reader
    # (an open browser, a second stage) turns a write into an immediate
    # "database is locked" error instead of a short wait.
    connection.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    return connection


def connect_db_readonly(path: str | Path) -> sqlite3.Connection:
    """Open an existing pipeline database without permitting writes."""
    database_path = Path(path)
    if not database_path.is_file():
        raise DatabaseError(f"Database does not exist: {database_path}")
    try:
        connection = sqlite3.connect(f"file:{database_path.resolve()}?mode=ro", uri=True)
    except sqlite3.Error as exc:
        raise DatabaseError(f"Failed to open database read-only: {database_path}") from exc
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    return connection


def _migration_1(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE pipeline_runs (
            id INTEGER PRIMARY KEY,
            run_uuid TEXT NOT NULL UNIQUE,
            topic TEXT NOT NULL,
            stage TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'running',
            config_hash TEXT NOT NULL,
            config_json TEXT,
            git_revision TEXT,
            package_versions_json TEXT,
            raw_json TEXT,
            started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at TEXT,
            error TEXT
        );

        CREATE INDEX IF NOT EXISTS ix_pipeline_runs_topic_stage
            ON pipeline_runs(topic, stage, status);

        CREATE TABLE search_windows (
            id INTEGER PRIMARY KEY,
            pipeline_run_id INTEGER REFERENCES pipeline_runs(id) ON DELETE SET NULL,
            topic TEXT NOT NULL,
            query_hash TEXT NOT NULL,
            request_fingerprint TEXT NOT NULL,
            query TEXT NOT NULL,
            platform TEXT NOT NULL,
            collection_ids_json TEXT NOT NULL,
            source_ids_json TEXT NOT NULL DEFAULT '[]',
            languages_json TEXT NOT NULL DEFAULT '[]',
            window_start TEXT NOT NULL,
            window_end TEXT NOT NULL,
            relevant_count INTEGER,
            total_count INTEGER,
            pages_fetched INTEGER NOT NULL DEFAULT 0,
            pagination_completed INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'pending',
            mediacloud_version TEXT NOT NULL,
            raw_json TEXT,
            completed_at TEXT,
            error TEXT,
            UNIQUE(topic, request_fingerprint, window_start, window_end)
        );

        CREATE INDEX IF NOT EXISTS ix_search_windows_run
            ON search_windows(pipeline_run_id);

        CREATE TABLE stories (
            story_id TEXT PRIMARY KEY,
            title TEXT,
            url TEXT,
            url_norm TEXT,
            domain TEXT,
            publish_date TEXT,
            media_name TEXT,
            media_url TEXT,
            language TEXT,
            indexed_at TEXT,
            mc_text TEXT,
            title_norm TEXT,
            raw_json TEXT,
            first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS ix_stories_url_norm ON stories(url_norm);
        CREATE INDEX IF NOT EXISTS ix_stories_title_norm ON stories(title_norm);

        -- Deduplication and QC verdicts depend on topic configuration (languages,
        -- domain_allowlist, exclude_url_patterns, exclude_title_terms, and date range),
        -- so they cannot live on the globally-keyed stories row: two topics that both
        -- return one story would overwrite each other's verdicts.
        CREATE TABLE story_topic_state (
            topic TEXT NOT NULL,
            story_id TEXT NOT NULL REFERENCES stories(story_id) ON DELETE CASCADE,
            qc_status TEXT,
            qc_reason TEXT,
            dup_of_story_id TEXT REFERENCES stories(story_id) ON DELETE SET NULL,
            decided_at TEXT,
            PRIMARY KEY (topic, story_id)
        );

        CREATE INDEX IF NOT EXISTS ix_story_topic_state_qc
            ON story_topic_state(topic, qc_status);
        CREATE INDEX IF NOT EXISTS ix_story_topic_state_duplicate
            ON story_topic_state(dup_of_story_id);

        CREATE TABLE search_hits (
            search_window_id INTEGER NOT NULL
                REFERENCES search_windows(id) ON DELETE CASCADE,
            story_id TEXT NOT NULL REFERENCES stories(story_id) ON DELETE CASCADE,
            page_number INTEGER NOT NULL DEFAULT 1,
            result_rank INTEGER,
            raw_json TEXT,
            found_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (search_window_id, story_id)
        );

        CREATE INDEX IF NOT EXISTS ix_search_hits_story ON search_hits(story_id);

        CREATE TABLE articles (
            id INTEGER PRIMARY KEY,
            story_id TEXT NOT NULL UNIQUE REFERENCES stories(story_id) ON DELETE CASCADE,
            fetch_status TEXT,
            source TEXT,
            http_status INTEGER,
            final_url TEXT,
            extractor TEXT,
            text TEXT,
            text_chars INTEGER,
            byline TEXT,
            extracted_date TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            raw_html_path TEXT,
            raw_json TEXT,
            fetched_at TEXT
        );

        CREATE INDEX IF NOT EXISTS ix_articles_fetch_status ON articles(fetch_status);

        CREATE TABLE fetch_attempts (
            id INTEGER PRIMARY KEY,
            article_id INTEGER NOT NULL REFERENCES articles(id) ON DELETE CASCADE,
            attempt_number INTEGER NOT NULL,
            source TEXT,
            request_url TEXT,
            final_url TEXT,
            http_status INTEGER,
            elapsed_ms INTEGER,
            retry_after_s REAL,
            extractor TEXT,
            text_chars INTEGER,
            error TEXT,
            raw_json TEXT,
            attempted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(article_id, attempt_number)
        );

        CREATE INDEX IF NOT EXISTS ix_fetch_attempts_article ON fetch_attempts(article_id);

        CREATE TABLE extraction_batches (
            id INTEGER PRIMARY KEY,
            pipeline_run_id INTEGER REFERENCES pipeline_runs(id) ON DELETE SET NULL,
            topic TEXT NOT NULL,
            model TEXT,
            base_url TEXT,
            prompt_version TEXT NOT NULL,
            prompt_hash TEXT NOT NULL,
            schema_hash TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            story_ids_json TEXT NOT NULL,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            request_json TEXT,
            response_json TEXT,
            usage_json TEXT,
            raw_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at TEXT,
            error TEXT
        );

        CREATE INDEX IF NOT EXISTS ix_extraction_batches_pending
            ON extraction_batches(topic, prompt_version, status);

        CREATE TABLE story_extractions (
            id INTEGER PRIMARY KEY,
            topic TEXT NOT NULL,
            story_id TEXT NOT NULL REFERENCES stories(story_id) ON DELETE CASCADE,
            extraction_batch_id INTEGER
                REFERENCES extraction_batches(id) ON DELETE SET NULL,
            model TEXT,
            prompt_version TEXT NOT NULL,
            relevant INTEGER,
            reject_reason TEXT,
            -- Schema-violating model output is persisted here with
            -- validation_status='invalid' and produces zero cases rows, so a bad
            -- response never aborts the surrounding batch transaction.
            validation_status TEXT
                CHECK (validation_status IN ('valid', 'invalid', 'unparsed')),
            validation_error TEXT,
            payload_json TEXT,
            usage_json TEXT,
            raw_json TEXT,
            extracted_at TEXT,
            UNIQUE(topic, story_id, prompt_version)
        );

        CREATE INDEX IF NOT EXISTS ix_story_extractions_batch
            ON story_extractions(extraction_batch_id);
        CREATE INDEX IF NOT EXISTS ix_story_extractions_topic
            ON story_extractions(topic, prompt_version, relevant);

        CREATE TABLE cases (
            id INTEGER PRIMARY KEY,
            case_id TEXT NOT NULL UNIQUE,
            story_extraction_id INTEGER NOT NULL
                REFERENCES story_extractions(id) ON DELETE CASCADE,
            case_type TEXT NOT NULL CHECK (case_type IN ('individual', 'cohort')),
            person_name TEXT,
            cohort_name TEXT,
            cohort_size INTEGER,
            cohort_period TEXT,
            link_category TEXT NOT NULL DEFAULT 'Revolving Door',
            previous_org TEXT,
            previous_role TEXT,
            previous_sector TEXT,
            previous_year INTEGER,
            current_org TEXT,
            current_role TEXT,
            current_sector TEXT,
            current_year INTEGER,
            direction TEXT,
            jurisdiction TEXT,
            expected_resolvable TEXT NOT NULL
                CHECK (expected_resolvable IN ('yes', 'partial', 'no')),
            status TEXT NOT NULL,
            source_outlet TEXT,
            source_url TEXT,
            claim TEXT NOT NULL,
            evidence_quote TEXT,
            evidence_verified INTEGER NOT NULL DEFAULT 0,
            raw_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS ix_cases_extraction ON cases(story_extraction_id);
        CREATE INDEX IF NOT EXISTS ix_cases_type_status ON cases(case_type, status);
        CREATE INDEX IF NOT EXISTS ix_cases_person_name ON cases(person_name);
        CREATE INDEX IF NOT EXISTS ix_cases_cohort_name ON cases(cohort_name);
        """
    )


def _migration_2(connection: sqlite3.Connection) -> None:
    connection.execute("ALTER TABLE search_windows ADD COLUMN next_pagination_token TEXT")


def _migration_3(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        ALTER TABLE cases ADD COLUMN private_org TEXT;
        ALTER TABLE cases ADD COLUMN private_time TEXT;
        ALTER TABLE cases ADD COLUMN public_org TEXT;
        ALTER TABLE cases ADD COLUMN public_time TEXT;
        """
    )


MIGRATIONS: dict[int, Callable[[sqlite3.Connection], None]] = {
    1: _migration_1,
    2: _migration_2,
    3: _migration_3,
}


@contextmanager
def transaction(connection: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Run a transaction, using a savepoint when already in one."""
    if connection.in_transaction:
        savepoint = f"mc_pipeline_{next(_SAVEPOINT_COUNTER)}"
        connection.execute(f"SAVEPOINT {savepoint}")
        try:
            yield connection
        except BaseException:
            connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            connection.execute(f"RELEASE SAVEPOINT {savepoint}")
            raise
        else:
            connection.execute(f"RELEASE SAVEPOINT {savepoint}")
        return

    connection.execute("BEGIN")
    try:
        yield connection
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def start_pipeline_run(
    connection: sqlite3.Connection,
    *,
    run_uuid: str,
    topic: str,
    stage: str,
    config_hash: str,
    config_json: str,
    git_revision: str | None,
    package_versions_json: str,
    raw_json: str | None = None,
) -> int:
    """Create a running provenance record and return its database identifier."""
    with transaction(connection):
        cursor = connection.execute(
            """
            INSERT INTO pipeline_runs(
                run_uuid, topic, stage, config_hash, config_json, git_revision,
                package_versions_json, raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_uuid,
                topic,
                stage,
                config_hash,
                config_json,
                git_revision,
                package_versions_json,
                raw_json,
            ),
        )
        if cursor.lastrowid is None:
            raise DatabaseError("Failed to create pipeline run.")
        return cursor.lastrowid


def complete_pipeline_run(
    connection: sqlite3.Connection,
    run_id: int,
    *,
    status: str,
    error: str | None = None,
) -> None:
    """Set a pipeline run's terminal status and completion timestamp."""
    with transaction(connection):
        cursor = connection.execute(
            """
            UPDATE pipeline_runs
            SET status = ?, error = ?, completed_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (status, error, run_id),
        )
        if cursor.rowcount != 1:
            raise DatabaseError(f"Pipeline run {run_id} does not exist.")


def topic_reachable_stories(connection: sqlite3.Connection, topic: str) -> list[sqlite3.Row]:
    """Return each story reached by a topic's search windows exactly once."""
    return connection.execute(
        """
        SELECT DISTINCT
            stories.story_id,
            stories.title,
            stories.url,
            stories.domain,
            stories.publish_date,
            stories.media_name,
            stories.language
        FROM search_hits
        JOIN search_windows ON search_windows.id = search_hits.search_window_id
        JOIN stories ON stories.story_id = search_hits.story_id
        WHERE search_windows.topic = ?
        ORDER BY stories.story_id ASC
        """,
        (topic,),
    ).fetchall()


def fetch_review_rows(connection: sqlite3.Connection, topic: str, limit: int) -> list[sqlite3.Row]:
    """Return a bounded newest-first Stage 3 review sample for one topic."""
    if not 1 <= limit <= MAX_FETCH_REVIEW_LIMIT:
        raise ValueError(f"limit must be between 1 and {MAX_FETCH_REVIEW_LIMIT}")
    return connection.execute(
        """
        WITH reachable_stories AS (
            SELECT DISTINCT search_hits.story_id
            FROM search_hits
            JOIN search_windows ON search_windows.id = search_hits.search_window_id
            WHERE search_windows.topic = ?
        )
        SELECT
            ? AS topic,
            stories.story_id,
            stories.title,
            stories.url,
            stories.domain,
            stories.publish_date,
            stories.media_name,
            articles.fetch_status,
            articles.source,
            articles.http_status,
            articles.final_url,
            articles.extractor,
            articles.text,
            articles.text_chars,
            articles.attempts,
            articles.error,
            articles.fetched_at
        FROM reachable_stories
        JOIN story_topic_state
            ON story_topic_state.topic = ?
           AND story_topic_state.story_id = reachable_stories.story_id
           AND story_topic_state.qc_status = 'ok'
        JOIN stories ON stories.story_id = reachable_stories.story_id
        JOIN articles
            ON articles.story_id = stories.story_id
           AND articles.fetch_status = 'ok'
        ORDER BY articles.fetched_at DESC, stories.story_id ASC
        LIMIT ?
        """,
        (topic, topic, topic, limit),
    ).fetchall()


def dedup_review_rows(connection: sqlite3.Connection, topic: str) -> list[sqlite3.Row]:
    """Return deterministic Stage 2 review rows for every reachable topic story."""
    return connection.execute(
        """
        WITH reachable_stories AS (
            SELECT DISTINCT search_hits.story_id
            FROM search_hits
            JOIN search_windows ON search_windows.id = search_hits.search_window_id
            WHERE search_windows.topic = ?
        )
        SELECT
            stories.story_id,
            stories.title,
            stories.url,
            stories.url_norm,
            stories.title_norm,
            stories.domain,
            stories.publish_date,
            stories.media_name,
            stories.media_url,
            stories.language,
            stories.indexed_at,
            stories.first_seen_at,
            stories.last_seen_at,
            story_topic_state.qc_status,
            story_topic_state.qc_reason,
            story_topic_state.dup_of_story_id,
            duplicate_story.title AS duplicate_of_title,
            duplicate_story.url AS duplicate_of_url,
            duplicate_story.publish_date AS duplicate_of_publish_date,
            story_topic_state.decided_at
        FROM reachable_stories
        JOIN stories ON stories.story_id = reachable_stories.story_id
        LEFT JOIN story_topic_state
            ON story_topic_state.topic = ?
           AND story_topic_state.story_id = stories.story_id
        LEFT JOIN stories AS duplicate_story
            ON duplicate_story.story_id = story_topic_state.dup_of_story_id
        ORDER BY stories.story_id ASC
        """,
        (topic, topic),
    ).fetchall()


def dedup_review_stats(connection: sqlite3.Connection, topic: str) -> Mapping[str, int]:
    """Return aggregate Stage 2 outcomes for a topic's reachable stories."""
    row = connection.execute(
        """
        WITH reachable_stories AS (
            SELECT DISTINCT search_hits.story_id
            FROM search_hits
            JOIN search_windows ON search_windows.id = search_hits.search_window_id
            WHERE search_windows.topic = ?
        )
        SELECT
            COUNT(*) AS story_count,
            COUNT(story_topic_state.story_id) AS decided_story_count,
            COALESCE(SUM(story_topic_state.qc_status = 'ok'), 0) AS accepted_story_count,
            COALESCE(SUM(story_topic_state.qc_status = 'dup'), 0) AS duplicate_story_count,
            COALESCE(
                SUM(
                    story_topic_state.qc_status IS NOT NULL
                    AND story_topic_state.qc_status NOT IN ('ok', 'dup')
                ),
                0
            ) AS rejected_story_count,
            COALESCE(SUM(story_topic_state.qc_reason = 'duplicate_url'), 0)
                AS duplicate_url_count,
            COALESCE(SUM(story_topic_state.qc_reason = 'duplicate_title'), 0)
                AS duplicate_title_count,
            COALESCE(SUM(story_topic_state.qc_status = 'sports_title'), 0)
                AS sports_title_count
        FROM reachable_stories
        LEFT JOIN story_topic_state
            ON story_topic_state.topic = ?
           AND story_topic_state.story_id = reachable_stories.story_id
        """,
        (topic, topic),
    ).fetchone()
    if row is None:
        raise DatabaseError("Failed to query dedup review statistics.")
    return dict(zip(row.keys(), (int(value) for value in row), strict=True))


def persist_topic_story_states(
    connection: sqlite3.Connection,
    *,
    topic: str,
    states: Sequence[Mapping[str, str | None]],
) -> None:
    """Atomically store a topic's current QC decisions and remove stale states."""
    with transaction(connection):
        connection.executemany(
            """
            UPDATE stories
            SET url_norm = ?, title_norm = ?
            WHERE story_id = ?
            """,
            [(state["url_norm"], state["title_norm"], state["story_id"]) for state in states],
        )
        connection.executemany(
            """
            INSERT INTO story_topic_state(
                topic, story_id, qc_status, qc_reason, dup_of_story_id, decided_at
            )
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(topic, story_id) DO UPDATE SET
                qc_status = excluded.qc_status,
                qc_reason = excluded.qc_reason,
                dup_of_story_id = excluded.dup_of_story_id,
                decided_at = CURRENT_TIMESTAMP
            WHERE story_topic_state.qc_status IS NOT excluded.qc_status
               OR story_topic_state.qc_reason IS NOT excluded.qc_reason
               OR story_topic_state.dup_of_story_id IS NOT excluded.dup_of_story_id
            """,
            [
                (
                    topic,
                    state["story_id"],
                    state["qc_status"],
                    state["qc_reason"],
                    state["dup_of_story_id"],
                )
                for state in states
            ],
        )
        connection.execute(
            """
            DELETE FROM story_topic_state
            WHERE topic = ?
              AND NOT EXISTS (
                  SELECT 1
                  FROM search_hits
                  JOIN search_windows ON search_windows.id = search_hits.search_window_id
                  WHERE search_windows.topic = ?
                    AND search_hits.story_id = story_topic_state.story_id
              )
            """,
            (topic, topic),
        )


def get_search_window(
    connection: sqlite3.Connection,
    *,
    topic: str,
    request_fingerprint: str,
    window_start: str,
    window_end: str,
) -> sqlite3.Row | None:
    """Return one cached search window identified by its request scope."""
    return cast(
        sqlite3.Row | None,
        connection.execute(
            """
        SELECT *
        FROM search_windows
        WHERE topic = ?
          AND request_fingerprint = ?
          AND window_start = ?
          AND window_end = ?
        """,
            (topic, request_fingerprint, window_start, window_end),
        ).fetchone(),
    )


def upsert_search_window(
    connection: sqlite3.Connection,
    *,
    pipeline_run_id: int,
    topic: str,
    query_hash: str,
    request_fingerprint: str,
    query: str,
    platform: str,
    collection_ids_json: str,
    source_ids_json: str,
    languages_json: str,
    window_start: str,
    window_end: str,
    mediacloud_version: str,
) -> int:
    """Create or attach the current run to a scoped search window."""
    with transaction(connection):
        connection.execute(
            """
            INSERT INTO search_windows(
                pipeline_run_id, topic, query_hash, request_fingerprint, query, platform,
                collection_ids_json, source_ids_json, languages_json, window_start, window_end,
                mediacloud_version
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(topic, request_fingerprint, window_start, window_end)
            DO UPDATE SET pipeline_run_id = excluded.pipeline_run_id
            """,
            (
                pipeline_run_id,
                topic,
                query_hash,
                request_fingerprint,
                query,
                platform,
                collection_ids_json,
                source_ids_json,
                languages_json,
                window_start,
                window_end,
                mediacloud_version,
            ),
        )
        row = get_search_window(
            connection,
            topic=topic,
            request_fingerprint=request_fingerprint,
            window_start=window_start,
            window_end=window_end,
        )
        if row is None:
            raise DatabaseError("Failed to create search window.")
        return int(row["id"])


def update_search_window_estimate(
    connection: sqlite3.Connection,
    window_id: int,
    *,
    relevant_count: int | None,
    total_count: int | None,
    raw_json: str,
) -> None:
    """Persist a search-count response for a window."""
    with transaction(connection):
        cursor = connection.execute(
            """
            UPDATE search_windows
            SET relevant_count = ?, total_count = ?, raw_json = ?
            WHERE id = ?
            """,
            (relevant_count, total_count, raw_json, window_id),
        )
        if cursor.rowcount != 1:
            raise DatabaseError(f"Search window {window_id} does not exist.")


def persist_search_page(
    connection: sqlite3.Connection,
    *,
    window_id: int,
    page_number: int,
    stories: Sequence[Mapping[str, Any]],
    next_pagination_token: str | None,
    window_raw_json: str,
    pagination_completed: bool = False,
) -> None:
    """Atomically persist one search page and its terminal or resume state."""
    if page_number < 1:
        raise ValueError("page_number must be at least 1.")

    with transaction(connection):
        for story in stories:
            story_id = story["story_id"]
            raw_json = story["raw_json"]
            connection.execute(
                """
                INSERT INTO stories(
                    story_id, title, url, url_norm, domain, publish_date, media_name,
                    media_url, language, indexed_at, mc_text, title_norm, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(story_id) DO UPDATE SET
                    title = COALESCE(excluded.title, stories.title),
                    url = COALESCE(excluded.url, stories.url),
                    url_norm = COALESCE(excluded.url_norm, stories.url_norm),
                    domain = COALESCE(excluded.domain, stories.domain),
                    publish_date = COALESCE(excluded.publish_date, stories.publish_date),
                    media_name = COALESCE(excluded.media_name, stories.media_name),
                    media_url = COALESCE(excluded.media_url, stories.media_url),
                    language = COALESCE(excluded.language, stories.language),
                    indexed_at = COALESCE(excluded.indexed_at, stories.indexed_at),
                    mc_text = COALESCE(excluded.mc_text, stories.mc_text),
                    title_norm = COALESCE(excluded.title_norm, stories.title_norm),
                    raw_json = excluded.raw_json,
                    last_seen_at = CURRENT_TIMESTAMP
                """,
                (
                    story_id,
                    story.get("title"),
                    story.get("url"),
                    story.get("url_norm"),
                    story.get("domain"),
                    story.get("publish_date"),
                    story.get("media_name"),
                    story.get("media_url"),
                    story.get("language"),
                    story.get("indexed_at"),
                    story.get("mc_text"),
                    story.get("title_norm"),
                    raw_json,
                ),
            )
            connection.execute(
                """
                INSERT INTO search_hits(
                    search_window_id, story_id, page_number, result_rank, raw_json
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(search_window_id, story_id) DO UPDATE SET
                    page_number = excluded.page_number,
                    result_rank = excluded.result_rank,
                    raw_json = excluded.raw_json,
                    found_at = CURRENT_TIMESTAMP
                """,
                (
                    window_id,
                    story_id,
                    page_number,
                    story.get("result_rank"),
                    raw_json,
                ),
            )

        if pagination_completed:
            cursor = connection.execute(
                """
                UPDATE search_windows
                SET pages_fetched = MAX(pages_fetched, ?),
                    next_pagination_token = NULL,
                    raw_json = ?,
                    status = 'completed',
                    pagination_completed = 1,
                    completed_at = CURRENT_TIMESTAMP,
                    error = NULL
                WHERE id = ?
                """,
                (page_number, window_raw_json, window_id),
            )
        else:
            cursor = connection.execute(
                """
                UPDATE search_windows
                SET pages_fetched = MAX(pages_fetched, ?),
                    next_pagination_token = ?,
                    raw_json = ?,
                    status = 'running',
                    pagination_completed = 0,
                    completed_at = NULL,
                    error = NULL
                WHERE id = ?
                """,
                (page_number, next_pagination_token, window_raw_json, window_id),
            )
        if cursor.rowcount != 1:
            raise DatabaseError(f"Search window {window_id} does not exist.")


def mark_search_window_failed(
    connection: sqlite3.Connection,
    window_id: int,
    *,
    status: str,
    error: str,
    raw_json: str,
) -> None:
    """Record a failed or page-limited window without discarding its resume token."""
    with transaction(connection):
        cursor = connection.execute(
            """
            UPDATE search_windows
            SET status = ?, raw_json = ?, error = ?, completed_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (status, raw_json, error, window_id),
        )
        if cursor.rowcount != 1:
            raise DatabaseError(f"Search window {window_id} does not exist.")


def search_review_rows(connection: sqlite3.Connection, topic: str) -> list[sqlite3.Row]:
    """Return deterministic, per-hit search rows for review."""
    return connection.execute(
        """
        SELECT
            search_windows.id AS search_window_id,
            search_windows.topic,
            search_windows.window_start,
            search_windows.window_end,
            search_windows.status AS window_status,
            search_windows.pagination_completed,
            search_hits.page_number,
            search_hits.result_rank,
            search_hits.raw_json AS hit_raw_json,
            stories.story_id,
            stories.title,
            stories.url,
            stories.url_norm,
            stories.domain,
            stories.publish_date,
            stories.media_name,
            stories.media_url,
            stories.language,
            stories.indexed_at,
            stories.mc_text,
            stories.title_norm,
            stories.raw_json,
            stories.first_seen_at,
            stories.last_seen_at
        FROM search_hits
        JOIN search_windows ON search_windows.id = search_hits.search_window_id
        JOIN stories ON stories.story_id = search_hits.story_id
        WHERE search_windows.topic = ?
        ORDER BY
            search_windows.window_start ASC,
            search_windows.window_end ASC,
            search_hits.page_number ASC,
            search_hits.result_rank ASC,
            stories.story_id ASC
        """,
        (topic,),
    ).fetchall()


def search_review_stats(connection: sqlite3.Connection, topic: str) -> Mapping[str, int]:
    """Return deterministic aggregate counts for a topic's search cache."""
    row = connection.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM search_windows WHERE topic = ?) AS window_count,
            (
                SELECT COUNT(*) FROM search_windows
                WHERE topic = ? AND status = 'completed'
            ) AS completed_window_count,
            (
                SELECT COUNT(*) FROM search_windows
                WHERE topic = ? AND status = 'failed'
            ) AS failed_window_count,
            (
                SELECT COUNT(*) FROM search_windows
                WHERE topic = ? AND status = 'page_limit'
            ) AS page_limit_window_count,
            (
                SELECT COALESCE(SUM(pages_fetched), 0) FROM search_windows WHERE topic = ?
            ) AS pages_fetched,
            (
                SELECT COUNT(*)
                FROM search_hits
                JOIN search_windows ON search_windows.id = search_hits.search_window_id
                WHERE search_windows.topic = ?
            ) AS hit_count,
            (
                SELECT COUNT(DISTINCT search_hits.story_id)
                FROM search_hits
                JOIN search_windows ON search_windows.id = search_hits.search_window_id
                WHERE search_windows.topic = ?
            ) AS story_count
        """,
        (topic, topic, topic, topic, topic, topic, topic),
    ).fetchone()
    if row is None:
        raise DatabaseError("Failed to query search review statistics.")
    return dict(zip(row.keys(), (int(value) for value in row), strict=True))


def init_db(path: str | Path) -> sqlite3.Connection:
    """Create or upgrade the pipeline database and return its connection.

    Migrations are tracked by SQLite's ``PRAGMA user_version`` and can be run
    repeatedly without changing an already-current database.
    """
    connection = connect_db(path)
    current_version = connection.execute("PRAGMA user_version").fetchone()[0]

    if current_version > SCHEMA_VERSION:
        connection.close()
        raise DatabaseError(
            f"Database schema version {current_version} is newer than supported "
            f"version {SCHEMA_VERSION}."
        )

    try:
        for version in range(current_version + 1, SCHEMA_VERSION + 1):
            migration = MIGRATIONS[version]
            with connection:
                migration(connection)
                connection.execute(f"PRAGMA user_version = {version}")
    except sqlite3.Error as exc:
        connection.close()
        raise DatabaseError(
            f"Failed to migrate database from version {current_version} "
            f"to version {SCHEMA_VERSION}."
        ) from exc

    return connection
