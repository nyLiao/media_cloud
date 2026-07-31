from __future__ import annotations

import csv

import pytest

import mc_pipeline.export as export_module
from mc_pipeline.config import AppConfig
from mc_pipeline.contracts import ARTICLE_AUDIT_COLUMNS, case_csv_columns
from mc_pipeline.db import init_db
from mc_pipeline.errors import ExportError
from mc_pipeline.export import export_topic


def _config() -> AppConfig:
    return AppConfig.model_validate(
        {
            "media_cloud": {
                "window_days": 1,
                "page_size": 1,
                "max_pages_per_window": 1,
                "timeout_s": 300,
                "max_retries": 0,
            },
            "fetch": {
                "user_agent": "test-agent",
                "per_domain_delay_s": 1,
                "global_max_rps": 1,
                "timeout_s": 1,
                "max_redirects": 1,
                "max_response_bytes": 1,
                "max_retries": 0,
                "respect_robots": True,
                "wayback_fallback": False,
                "min_text_chars": 1,
                "save_raw_html": False,
            },
            "llm": {
                "model": "test-model",
                "timeout_s": 300,
                "max_requests_per_min": 1,
                "max_input_tokens": 1,
                "max_output_tokens": 1,
                "temperature": 0,
                "structured_output": "json_schema",
                "prompt_version": "v1",
            },
            "topics": {
                "door": {
                    "label": "Door",
                    "link_category": "Revolving Door",
                    "query": "door",
                    "start_date": "2021-01-01",
                    "end_date": "2021-01-02",
                    "collection_ids": [1],
                    "languages": ["en"],
                    "domain_allowlist": [],
                    "qc": {"min_title_chars": 0, "exclude_url_patterns": []},
                    "extraction": {
                        "unit": "person_transition",
                        "relevance_criteria": "test",
                        "fields": [
                            {
                                "name": "person_name",
                                "type": "string",
                                "required": False,
                                "description": "Person",
                            },
                            {
                                "name": "private_org",
                                "type": "string",
                                "required": False,
                                "description": "Private organization",
                            },
                            {
                                "name": "private_time",
                                "type": "string",
                                "required": False,
                                "description": "Private time",
                            },
                            {
                                "name": "public_org",
                                "type": "string",
                                "required": False,
                                "description": "Public organization",
                            },
                            {
                                "name": "public_time",
                                "type": "string",
                                "required": False,
                                "description": "Public time",
                            },
                            {
                                "name": "jurisdiction",
                                "type": "string",
                                "required": False,
                                "description": "Jurisdiction",
                            },
                        ],
                    },
                }
            },
        }
    )


def _reachable_story(
    connection, story_id: str, publish_date: str | None, title: str | None
) -> None:
    connection.execute(
        """
        INSERT INTO stories(story_id, title, url, domain, publish_date, media_name, raw_json)
        VALUES (?, ?, ?, 'example.test', ?, 'Example News', '{}')
        """,
        (story_id, title, f"https://example.test/{story_id}", publish_date),
    )
    window_id = connection.execute(
        """
        INSERT INTO search_windows(
            topic, query_hash, request_fingerprint, query, platform,
            collection_ids_json, window_start, window_end, mediacloud_version
        )
        VALUES ('door', ?, ?, 'door', 'test', '[1]', '2021-01-01', '2021-01-02', 'test')
        """,
        (f"query-{story_id}", f"fingerprint-{story_id}"),
    ).lastrowid
    connection.execute(
        "INSERT INTO search_hits(search_window_id, story_id, raw_json) VALUES (?, ?, '{}')",
        (window_id, story_id),
    )


def test_export_writes_configured_cases_and_all_reachable_article_rows(tmp_path):
    config = _config()
    connection = init_db(tmp_path / "pipeline.db")
    try:
        _reachable_story(connection, "story-2", "2021-01-02", 'A "quoted", title')
        _reachable_story(connection, "story-1", "2021-01-01", "Earlier story")
        connection.executemany(
            """
            INSERT INTO story_topic_state(topic, story_id, qc_status, qc_reason, dup_of_story_id)
            VALUES ('door', ?, ?, ?, ?)
            """,
            [
                ("story-1", "ok", None, None),
                ("story-2", "dup", "duplicate_url", "story-1"),
            ],
        )
        connection.execute(
            """
            INSERT INTO articles(story_id, fetch_status, source, text_chars)
            VALUES ('story-1', 'ok', 'web', 42)
            """
        )
        connection.execute(
            """
            INSERT INTO story_extractions(
                topic, story_id, prompt_version, relevant, validation_status, extracted_at
            )
            VALUES ('door', 'story-1', ?, 1, 'valid', '2021-01-02T00:00:00Z')
            """,
            (config.llm.screening_prompt_version,),
        )
        extraction_id = connection.execute(
            """
            INSERT INTO story_extractions(
                topic, story_id, model, prompt_version, relevant, extracted_at
            )
            VALUES ('door', 'story-1', 'test-model', 'v1', 1, '2021-01-03T00:00:00Z')
            """
        ).lastrowid
        connection.execute(
            """
            INSERT INTO cases(
                case_id, story_extraction_id, case_type, person_name, private_org, private_time,
                public_org, public_time, jurisdiction, expected_resolvable, status, claim,
                evidence_verified
            )
            VALUES ('door:story-1:v1:01', ?, 'individual', 'Ada, "Ace"', 'Example LLC', NULL,
                    'Government', '2021', 'Canada', 'yes', 'reported', 'A claim', 1)
            """,
            (extraction_id,),
        )
        connection.commit()

        summary = export_topic(config, "door", connection, output_dir=tmp_path / "out")

        with (tmp_path / "out" / "door.csv").open(encoding="utf-8", newline="") as handle:
            cases = list(csv.DictReader(handle))
            assert tuple(cases[0]) == case_csv_columns(config.topics["door"])
        with (tmp_path / "out" / "door_articles.csv").open(encoding="utf-8", newline="") as handle:
            articles = list(csv.DictReader(handle))

        assert summary.processed == summary.succeeded == 2
        assert len(cases) == 1
        assert cases[0]["person_name"] == 'Ada, "Ace"'
        assert cases[0]["private_time"] == ""
        assert cases[0]["source_outlet"] == "Example News"
        assert cases[0]["source_url"] == "https://example.test/story-1"
        assert tuple(articles[0]) == ARTICLE_AUDIT_COLUMNS
        assert [row["story_id"] for row in articles] == ["story-1", "story-2"]
        assert articles[0]["screening_relevant"] == "1"
        assert articles[0]["screening_validation_status"] == "valid"
        assert articles[0]["screening_prompt_version"] == config.llm.screening_prompt_version
        assert articles[1]["fetch_status"] == ""
        assert articles[1]["relevant"] == ""
        assert articles[1]["dup_of_story_id"] == "story-1"
    finally:
        connection.close()


def test_export_does_not_replace_existing_files_when_reconciliation_fails(tmp_path, monkeypatch):
    config = _config()
    connection = init_db(tmp_path / "pipeline.db")
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    case_destination = output_dir / "door.csv"
    audit_destination = output_dir / "door_articles.csv"
    case_destination.write_text("old cases\n", encoding="utf-8")
    audit_destination.write_text("old articles\n", encoding="utf-8")
    try:
        _reachable_story(connection, "story-1", "2021-01-01", "A story")
        connection.commit()

        original_write_csv = export_module._write_csv

        def write_missing_row(destination, columns, rows):
            temporary, count = original_write_csv(destination, columns, rows)
            return temporary, count - 1 if destination == audit_destination else count

        monkeypatch.setattr(export_module, "_write_csv", write_missing_row)

        with pytest.raises(ExportError, match="reconciliation"):
            export_topic(config, "door", connection, output_dir=output_dir)

        assert case_destination.read_text(encoding="utf-8") == "old cases\n"
        assert audit_destination.read_text(encoding="utf-8") == "old articles\n"
        assert not list(output_dir.glob("*.tmp"))
    finally:
        connection.close()
