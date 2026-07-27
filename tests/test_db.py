from __future__ import annotations

import json
import sqlite3

import pytest
from typer.testing import CliRunner

from mc_pipeline.cli import app
from mc_pipeline.db import SCHEMA_VERSION, init_db


def test_init_db_creates_schema(tmp_path):
    connection = init_db(tmp_path / "pipeline.db")
    try:
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        assert {
            "pipeline_runs",
            "search_windows",
            "stories",
            "search_hits",
            "articles",
            "fetch_attempts",
            "extraction_batches",
            "story_extractions",
            "cases",
        } <= tables
        assert connection.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
    finally:
        connection.close()


def test_init_db_migration_is_idempotent(tmp_path):
    database_path = tmp_path / "pipeline.db"
    first = init_db(database_path)
    first.execute(
        """
        INSERT INTO pipeline_runs(run_uuid, topic, stage, config_hash)
        VALUES ('run-1', 'door', 'search', 'config-hash')
        """
    )
    first.commit()
    first.close()

    second = init_db(database_path)
    try:
        assert second.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        assert second.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0] == 1
    finally:
        second.close()


def test_connection_enables_wal_and_foreign_keys(tmp_path):
    connection = init_db(tmp_path / "pipeline.db")
    try:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO search_hits(search_window_id, story_id, raw_json) "
                "VALUES (999, 'missing', '{}')"
            )
    finally:
        connection.close()


def test_duplicate_story_can_link_to_multiple_windows(tmp_path):
    connection = init_db(tmp_path / "pipeline.db")
    try:
        connection.execute(
            "INSERT INTO stories(story_id, title, raw_json) VALUES (?, ?, ?)",
            ("story-1", "A story", "{}"),
        )
        for start, end in (("2026-01-01", "2026-01-31"), ("2026-02-01", "2026-02-28")):
            connection.execute(
                """
                INSERT INTO search_windows(
                    topic, query_hash, request_fingerprint, query, platform,
                    collection_ids_json, window_start, window_end, mediacloud_version
                )
                VALUES ('door', 'query-hash', ?, 'query', 'onlinenews-mediacloud',
                        '[34411583]', ?, ?, '5.1.0')
                """,
                (f"fingerprint-{start}", start, end),
            )
        window_ids = [row[0] for row in connection.execute("SELECT id FROM search_windows")]
        connection.executemany(
            "INSERT INTO search_hits(search_window_id, story_id, raw_json) VALUES (?, ?, ?)",
            [(window_id, "story-1", "{}") for window_id in window_ids],
        )

        assert (
            connection.execute(
                "SELECT COUNT(*) FROM search_hits WHERE story_id = 'story-1'"
            ).fetchone()[0]
            == 2
        )
    finally:
        connection.close()


def test_multiple_cases_can_belong_to_one_story(tmp_path):
    connection = init_db(tmp_path / "pipeline.db")
    try:
        connection.execute("INSERT INTO stories(story_id, raw_json) VALUES ('story-1', '{}')")
        cursor = connection.execute(
            """
            INSERT INTO story_extractions(story_id, prompt_version, relevant)
            VALUES ('story-1', 'v1', 1)
            """
        )
        extraction_id = cursor.lastrowid
        connection.executemany(
            """
            INSERT INTO cases(
                case_id, story_extraction_id, case_type, person_name,
                expected_resolvable, status, claim
            )
            VALUES (?, ?, 'individual', ?, 'partial', 'reported', ?)
            """,
            [
                ("case-1", extraction_id, "Ada Example", "Ada changed sectors."),
                ("case-2", extraction_id, "Bea Example", "Bea changed sectors."),
            ],
        )

        assert (
            connection.execute(
                "SELECT COUNT(*) FROM cases WHERE story_extraction_id = ?",
                (extraction_id,),
            ).fetchone()[0]
            == 2
        )
    finally:
        connection.close()


def test_raw_json_round_trips(tmp_path):
    connection = init_db(tmp_path / "pipeline.db")
    payload = {"api": {"identifiers": [1, 2]}, "title": "A story"}
    try:
        connection.execute(
            "INSERT INTO stories(story_id, raw_json) VALUES (?, ?)",
            ("story-1", json.dumps(payload)),
        )
        stored_payload = connection.execute(
            "SELECT raw_json FROM stories WHERE story_id = 'story-1'"
        ).fetchone()[0]
        assert json.loads(stored_payload) == payload
    finally:
        connection.close()


def test_case_schema_supports_individual_and_cohort_gold_patterns(tmp_path):
    connection = init_db(tmp_path / "pipeline.db")
    try:
        connection.execute("INSERT INTO stories(story_id, raw_json) VALUES ('story-1', '{}')")
        extraction_id = connection.execute(
            """
            INSERT INTO story_extractions(story_id, prompt_version, relevant)
            VALUES ('story-1', 'v1', 1)
            """
        ).lastrowid
        connection.executemany(
            """
            INSERT INTO cases(
                case_id, story_extraction_id, case_type, person_name, cohort_name,
                cohort_size, cohort_period, previous_org, current_role, jurisdiction,
                expected_resolvable, status, source_outlet, source_url, claim
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "GC008",
                    extraction_id,
                    "individual",
                    "Moe Sihota",
                    None,
                    None,
                    None,
                    "Government of British Columbia",
                    "Consultant lobbyist",
                    "British Columbia",
                    "partial",
                    "reported",
                    "IJF",
                    "https://example.test/individual",
                    "Former cabinet minister later worked as a consultant lobbyist.",
                ),
                (
                    "GC011",
                    extraction_id,
                    "cohort",
                    None,
                    "Lobbyists at fundraisers",
                    166,
                    "2019-2023",
                    None,
                    None,
                    "federal",
                    "partial",
                    "reported",
                    "IJF",
                    "https://example.test/cohort",
                    "A reported cohort-level revolving-door pattern.",
                ),
            ],
        )

        rows = connection.execute(
            "SELECT case_id, case_type, person_name, cohort_size FROM cases ORDER BY case_id"
        ).fetchall()
        assert [tuple(row) for row in rows] == [
            ("GC008", "individual", "Moe Sihota", None),
            ("GC011", "cohort", None, 166),
        ]
    finally:
        connection.close()


def test_init_db_cli_command(tmp_path):
    database_path = tmp_path / "cli.db"
    result = CliRunner().invoke(app, ["init-db", "--db", str(database_path)])

    assert result.exit_code == 0
    assert database_path.exists()
    assert "schema version 1" in result.stdout
