"""SQLite connection helpers and schema migrations for the pipeline cache."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from pathlib import Path

from .errors import DatabaseError

SCHEMA_VERSION = 1
BUSY_TIMEOUT_MS = 5000


def connect_db(path: str | Path) -> sqlite3.Connection:
    """Open a configured SQLite connection at *path*."""
    database_path = Path(path)
    database_path.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA synchronous = NORMAL")
    # WAL still serializes writers; without a busy timeout any concurrent reader
    # (an open browser, a second stage) turns a write into an immediate
    # "database is locked" error instead of a short wait.
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
        -- domain_allowlist, exclude_url_patterns, date range, near-duplicate blocking),
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


MIGRATIONS: dict[int, Callable[[sqlite3.Connection], None]] = {1: _migration_1}


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
