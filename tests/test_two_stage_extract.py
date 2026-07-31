from __future__ import annotations

import json

import pytest
from pydantic import SecretStr

from mc_pipeline.config import LLMCredentials, load_config
from mc_pipeline.db import init_db
from mc_pipeline.extract import Candidate, build_manual_request, extract_topic, pack_candidates
from mc_pipeline.llm import LLMResponse
from mc_pipeline.prompt import (
    EXTRACTION_SYSTEM_PROMPT,
    SCREENING_SYSTEM_PROMPT,
    PromptArticle,
    build_extraction_prompt,
    build_screening_prompt,
)


class FakeEstimator:
    def count(self, text: str) -> int:
        return len(text)

    def truncate(self, text: str, max_tokens: int) -> str:
        return text[:max_tokens]


class FakeLimiter:
    def __init__(self) -> None:
        self.calls = 0

    def acquire(self) -> None:
        self.calls += 1


class FakeClient:
    def __init__(self, payloads: list[dict[str, object]]) -> None:
        self.payloads = payloads
        self.calls: list[tuple[str, str, dict[str, object]]] = []

    def extract(self, *, system_prompt, user_prompt, schema):
        self.calls.append((system_prompt, user_prompt, schema))
        return LLMResponse(
            content=json.dumps(self.payloads.pop(0)),
            request_id="request-1",
            usage={"prompt_tokens": 10, "completion_tokens": 5},
            raw={"id": "response-1"},
            structured_output="json_schema",
        )


def _seed_article(
    connection, story_id: str, *, title: str = "Transition", text: str = "Text"
) -> None:
    connection.execute(
        """INSERT INTO stories(story_id, title, url, domain, publish_date, media_name, raw_json)
           VALUES (?, ?, ?, 'example.test', '2024-01-01', 'Example News', '{}')""",
        (story_id, title, f"https://example.test/{story_id}"),
    )
    connection.execute(
        "INSERT INTO story_topic_state(topic, story_id, qc_status) VALUES (?, ?, 'ok')",
        ("revolving_door_ca", story_id),
    )
    connection.execute(
        """INSERT INTO articles(story_id, fetch_status, source, text, text_chars)
           VALUES (?, 'ok', 'url', ?, ?)""",
        (story_id, text, len(text)),
    )
    connection.commit()


def _case() -> dict[str, object]:
    return {
        "person_name": "Ada Example",
        "private_org": "Example Strategies",
        "private_time": "2024-2024",
        "public_org": "Government of Canada",
        "public_time": "2024-2024",
        "jurisdiction": "Ontario",
    }


def test_prompts_are_concise_phase_specific_and_cover_all_supported_cases():
    topic = load_config().topics["revolving_door_ca"]
    article = PromptArticle("story-1", "Quoted title", "Stored article text", 2024)

    screening = build_screening_prompt(topic, [article])
    detailed = build_extraction_prompt(topic, [article])

    assert '"relevant"' in screening
    assert '"reject_reason"' not in screening
    assert '"relevant"' not in detailed
    assert '"reject_reason"' not in detailed
    assert "publication_year: 2024" in detailed
    assert "PHASE" not in screening
    assert "PHASE" not in detailed
    assert "OUTPUT SCHEMA" not in screening
    assert "OUTPUT SCHEMA" not in detailed
    assert "cohort" not in screening
    assert "Return every distinct supported case individually" in detailed
    assert "jurisdiction must be one of: Alberta, British Columbia" in detailed
    assert "put Canada if no evidence" in detailed
    assert "Never output a city, or a municipality" in detailed


def test_extraction_batches_never_exceed_ten_articles():
    topic = load_config().topics["revolving_door_ca"]
    candidates = [Candidate(f"story-{index}", "Title", "text", 2024) for index in range(11)]

    batches = pack_candidates(
        candidates, topic=topic, estimator=FakeEstimator(), max_input_tokens=100_000
    )

    assert [len(batch.articles) for batch in batches] == [10, 1]
    assert all(batch.phase == "extraction" for batch in batches)


def test_extract_runs_screening_then_detailed_positive_only_with_provenance_and_time_fallback(
    tmp_path,
):
    config = load_config()
    connection = init_db(tmp_path / "pipeline.db")
    try:
        _seed_article(connection, "story-1", title="Lobby transition", text="Text one " * 100)
        _seed_article(connection, "story-2", title="Other story", text="Text two " * 100)
        client = FakeClient(
            [
                {
                    "decisions": [
                        {"story_id": "story-1", "relevant": True},
                        {"story_id": "story-2", "relevant": False},
                    ]
                },
                {
                    "results": [
                        {
                            "story_id": "story-1",
                            "cases": [_case()],
                        }
                    ]
                },
            ]
        )
        limiter = FakeLimiter()

        summary = extract_topic(
            config,
            "revolving_door_ca",
            connection,
            limit=2,
            credentials=LLMCredentials(base_url="https://llm.test/v1", api_key=SecretStr("x")),
            _client=client,
            _rate_limiter=limiter,
            _exporter=lambda: None,
        )

        screenings = connection.execute(
            """
            SELECT story_id, relevant, validation_status, prompt_version
            FROM story_extractions WHERE prompt_version = ? ORDER BY story_id
            """,
            (config.llm.screening_prompt_version,),
        ).fetchall()
        assert [tuple(row) for row in screenings] == [
            ("story-1", 1, "valid", config.llm.screening_prompt_version),
            ("story-2", 0, "valid", config.llm.screening_prompt_version),
        ]
        extraction = connection.execute(
            """
            SELECT story_id, relevant, reject_reason, prompt_version
            FROM story_extractions WHERE prompt_version = ?
            """,
            (config.llm.prompt_version,),
        ).fetchone()
        assert tuple(extraction) == (
            "story-1",
            1,
            None,
            config.llm.prompt_version,
        )
        assert tuple(
            connection.execute(
                "SELECT private_time, public_time, jurisdiction FROM cases"
            ).fetchone()
        ) == ("2024-2024", "2024-2024", "Ontario")
        assert [
            tuple(row)
            for row in connection.execute(
                """
                SELECT json_extract(request_json, '$.phase'), prompt_version
                FROM extraction_batches ORDER BY id
                """
            ).fetchall()
        ] == [
            ("screening", config.llm.screening_prompt_version),
            ("extraction", config.llm.prompt_version),
        ]
        assert [call[0] for call in client.calls] == [
            SCREENING_SYSTEM_PROMPT,
            EXTRACTION_SYSTEM_PROMPT,
        ]
        assert limiter.calls == 2
        assert summary.failed == 0
    finally:
        connection.close()


def test_stage_two_persists_omitted_article_as_valid_irrelevant_record(tmp_path):
    config = load_config()
    connection = init_db(tmp_path / "pipeline.db")
    try:
        _seed_article(connection, "story-1")
        client = FakeClient(
            [
                {"decisions": [{"story_id": "story-1", "relevant": True}]},
                {"results": []},
            ]
        )
        extract_topic(
            config,
            "revolving_door_ca",
            connection,
            limit=1,
            credentials=LLMCredentials(base_url="https://llm.test/v1", api_key=SecretStr("x")),
            _client=client,
            _rate_limiter=FakeLimiter(),
            _exporter=lambda: None,
        )

        assert tuple(
            connection.execute(
                """
                SELECT relevant, reject_reason FROM story_extractions
                WHERE prompt_version = ?
                """,
                (config.llm.prompt_version,),
            ).fetchone()
        ) == (0, None)
        assert connection.execute("SELECT COUNT(*) FROM cases").fetchone()[0] == 0
    finally:
        connection.close()


@pytest.mark.parametrize("validation_status", ["invalid", "unparsed"])
def test_normal_extract_reruns_invalid_detailed_extraction(validation_status, tmp_path):
    config = load_config()
    connection = init_db(tmp_path / "pipeline.db")
    try:
        _seed_article(connection, "story-1")
        connection.executemany(
            """
            INSERT INTO story_extractions(
                topic, story_id, prompt_version, relevant, validation_status
            ) VALUES ('revolving_door_ca', 'story-1', ?, ?, ?)
            """,
            [
                (config.llm.screening_prompt_version, 1, "valid"),
                (config.llm.prompt_version, None, validation_status),
            ],
        )
        connection.commit()
        client = FakeClient([{"results": [{"story_id": "story-1", "cases": [_case()]}]}])

        summary = extract_topic(
            config,
            "revolving_door_ca",
            connection,
            limit=1,
            credentials=LLMCredentials(base_url="https://llm.test/v1", api_key=SecretStr("x")),
            _client=client,
            _rate_limiter=FakeLimiter(),
            _exporter=lambda: None,
        )

        assert summary.failed == 0
        assert [call[0] for call in client.calls] == [EXTRACTION_SYSTEM_PROMPT]
        assert tuple(
            connection.execute(
                """
                SELECT relevant, validation_status FROM story_extractions
                WHERE prompt_version = ?
                """,
                (config.llm.prompt_version,),
            ).fetchone()
        ) == (1, "valid")
    finally:
        connection.close()


def test_dry_run_plans_screening_without_writes(tmp_path):
    config = load_config()
    connection = init_db(tmp_path / "pipeline.db")
    try:
        _seed_article(connection, "story-1")
        before = "\n".join(connection.iterdump())

        summary = extract_topic(config, "revolving_door_ca", connection, limit=1, dry_run=True)

        assert summary.skipped == 1
        assert '"phase":"screening"' in summary.request_details[0]
        assert "\n".join(connection.iterdump()) == before
    finally:
        connection.close()


def test_manual_request_reconstructs_exact_stored_phase_batch(tmp_path):
    config = load_config()
    connection = init_db(tmp_path / "pipeline.db")
    try:
        cursor = connection.execute(
            """INSERT INTO extraction_batches(
               topic, model, prompt_version, prompt_hash, schema_hash,
               status, story_ids_json, request_json)
               VALUES (?, ?, ?, 'prompt-hash', 'schema-hash', 'completed', ?, ?)""",
            (
                "revolving_door_ca",
                config.llm.model,
                config.llm.prompt_version,
                json.dumps(["story-1"]),
                json.dumps(
                    {
                        "system_prompt": "stored system",
                        "user_prompt": "stored user",
                        "estimated_input_tokens": 321,
                        "phase": "screening",
                    }
                ),
            ),
        )
        connection.commit()

        artifact = build_manual_request(
            config, "revolving_door_ca", connection, extraction_batch_id=int(cursor.lastrowid)
        )

        assert artifact["phase"] == "screening"
        assert artifact["system_prompt"] == "stored system"
        assert artifact["story_ids"] == ["story-1"]
    finally:
        connection.close()
