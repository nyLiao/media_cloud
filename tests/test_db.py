from __future__ import annotations

import json
import sqlite3

import pytest
from typer.testing import CliRunner

from mc_pipeline.cli import app
from mc_pipeline.db import BUSY_TIMEOUT_MS, SCHEMA_VERSION, init_db
from mc_pipeline.errors import DatabaseError


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
            "story_topic_state",
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


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}


def test_stories_holds_no_topic_scoped_verdicts(tmp_path):
    """QC and dedup verdicts depend on topic config, so they cannot live on stories."""
    connection = init_db(tmp_path / "pipeline.db")
    try:
        story_columns = _columns(connection, "stories")
        assert not story_columns & {"qc_status", "qc_reason", "dup_of_story_id"}
        assert {"qc_status", "qc_reason", "dup_of_story_id"} <= _columns(
            connection, "story_topic_state"
        )
    finally:
        connection.close()


def test_two_topics_hold_independent_qc_verdicts_for_one_story(tmp_path):
    connection = init_db(tmp_path / "pipeline.db")
    try:
        connection.executemany(
            "INSERT INTO stories(story_id, raw_json) VALUES (?, '{}')",
            [("story-1",), ("story-2",)],
        )
        connection.executemany(
            """
            INSERT INTO story_topic_state(topic, story_id, qc_status, dup_of_story_id)
            VALUES (?, ?, ?, ?)
            """,
            [
                ("revolving_door_ca", "story-1", "ok", None),
                ("patronage_ca", "story-1", "dup", "story-2"),
            ],
        )

        verdicts = connection.execute(
            "SELECT topic, qc_status, dup_of_story_id FROM story_topic_state "
            "WHERE story_id = 'story-1' ORDER BY topic"
        ).fetchall()
        assert [tuple(row) for row in verdicts] == [
            ("patronage_ca", "dup", "story-2"),
            ("revolving_door_ca", "ok", None),
        ]
    finally:
        connection.close()


def test_two_topics_hold_independent_extractions_at_the_same_prompt_version(tmp_path):
    connection = init_db(tmp_path / "pipeline.db")
    try:
        connection.execute("INSERT INTO stories(story_id, raw_json) VALUES ('story-1', '{}')")
        connection.executemany(
            """
            INSERT INTO story_extractions(topic, story_id, prompt_version, relevant)
            VALUES (?, 'story-1', 'v1', ?)
            """,
            [("revolving_door_ca", 1), ("patronage_ca", 0)],
        )

        assert (
            connection.execute(
                "SELECT COUNT(*) FROM story_extractions WHERE story_id = 'story-1'"
            ).fetchone()[0]
            == 2
        )

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO story_extractions(topic, story_id, prompt_version, relevant)
                VALUES ('revolving_door_ca', 'story-1', 'v1', 1)
                """
            )
    finally:
        connection.close()


def test_invalid_model_output_is_persisted_without_cases(tmp_path):
    """Schema-violating output is recorded, not dropped, and writes no cases row."""
    connection = init_db(tmp_path / "pipeline.db")
    try:
        connection.execute("INSERT INTO stories(story_id, raw_json) VALUES ('story-1', '{}')")
        connection.execute(
            """
            INSERT INTO story_extractions(
                topic, story_id, prompt_version, relevant,
                validation_status, validation_error, payload_json
            )
            VALUES ('revolving_door_ca', 'story-1', 'v1', 1, 'invalid',
                    "expected_resolvable: unexpected value 'unknown'",
                    '{"expected_resolvable": "unknown"}')
            """
        )
        connection.commit()

        row = connection.execute(
            "SELECT validation_status, payload_json FROM story_extractions"
        ).fetchone()
        assert row[0] == "invalid"
        assert json.loads(row[1]) == {"expected_resolvable": "unknown"}
        assert connection.execute("SELECT COUNT(*) FROM cases").fetchone()[0] == 0
    finally:
        connection.close()


def test_validation_status_rejects_unknown_values(tmp_path):
    connection = init_db(tmp_path / "pipeline.db")
    try:
        connection.execute("INSERT INTO stories(story_id, raw_json) VALUES ('story-1', '{}')")
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO story_extractions(topic, story_id, prompt_version, validation_status)
                VALUES ('revolving_door_ca', 'story-1', 'v1', 'probably-fine')
                """
            )
    finally:
        connection.close()


def test_connection_sets_busy_timeout(tmp_path):
    connection = init_db(tmp_path / "pipeline.db")
    try:
        assert connection.execute("PRAGMA busy_timeout").fetchone()[0] == BUSY_TIMEOUT_MS
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


def test_migration_fails_on_schema_drift_without_advancing_version(tmp_path):
    database_path = tmp_path / "pipeline.db"
    connection = sqlite3.connect(database_path)
    connection.execute("CREATE TABLE stories (story_id TEXT PRIMARY KEY)")
    connection.commit()
    connection.close()

    with pytest.raises(DatabaseError, match="Failed to migrate") as error:
        init_db(database_path)

    assert isinstance(error.value.__cause__, sqlite3.OperationalError)
    check = sqlite3.connect(database_path)
    try:
        assert check.execute("PRAGMA user_version").fetchone()[0] == 0
    finally:
        check.close()


def test_newer_database_version_raises_typed_error(tmp_path):
    database_path = tmp_path / "pipeline.db"
    connection = sqlite3.connect(database_path)
    connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION + 1}")
    connection.close()

    with pytest.raises(DatabaseError, match="newer than supported"):
        init_db(database_path)


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
            INSERT INTO story_extractions(topic, story_id, prompt_version, relevant)
            VALUES ('revolving_door_ca', 'story-1', 'v1', 1)
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
            INSERT INTO story_extractions(topic, story_id, prompt_version, relevant)
            VALUES ('revolving_door_ca', 'story-1', 'v1', 1)
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
