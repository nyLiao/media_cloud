from __future__ import annotations

import json

import pytest
from pydantic import SecretStr

from mc_pipeline.config import LLMConfig, LLMCredentials, load_config
from mc_pipeline.db import init_db
from mc_pipeline.errors import ConfigError, ExtractionError
from mc_pipeline.extract import (
    Candidate,
    _candidate_rows,
    build_manual_request,
    extract_topic,
    pack_candidates,
    write_manual_request,
)
from mc_pipeline.llm import LLMResponse
from mc_pipeline.prompt import SYSTEM_PROMPT, PromptArticle, build_extraction_prompt


class FakeEstimator:
    def count(self, text: str) -> int:
        return len(text)

    def truncate(self, text: str, max_tokens: int) -> str:
        return text[:max_tokens]


class FakeManualRequestEstimator(FakeEstimator):
    def __init__(self, *args, **kwargs) -> None:
        pass

    def count(self, text: str) -> int:
        return 100 * text.count("--- ARTICLE START ---")


class FakeLimiter:
    def __init__(self) -> None:
        self.calls = 0

    def acquire(self) -> None:
        self.calls += 1


class FakeClient:
    def __init__(self, payloads: list[dict[str, object] | LLMResponse | Exception]) -> None:
        self.payloads = payloads
        self.calls: list[tuple[str, str, dict[str, object]]] = []

    def extract(self, *, system_prompt, user_prompt, schema):
        self.calls.append((system_prompt, user_prompt, schema))
        payload = self.payloads.pop(0)
        if isinstance(payload, Exception):
            raise payload
        if isinstance(payload, LLMResponse):
            return payload
        return LLMResponse(
            content=json.dumps(payload),
            request_id="request-1",
            usage={"prompt_tokens": 10, "completion_tokens": 5},
            raw={"id": "response-1"},
            structured_output="json_schema",
        )


def _seed_article(connection, story_id: str, *, title: str, text: str) -> None:
    connection.execute(
        """
        INSERT INTO stories(story_id, title, url, domain, publish_date, media_name, raw_json)
        VALUES (?, ?, ?, 'example.test', '2024-01-01', 'Example News', '{}')
        """,
        (story_id, title, f"https://example.test/{story_id}"),
    )
    connection.execute(
        "INSERT INTO story_topic_state(topic, story_id, qc_status) VALUES (?, ?, 'ok')",
        ("revolving_door_ca", story_id),
    )
    connection.execute(
        """
        INSERT INTO articles(story_id, fetch_status, source, text, text_chars)
        VALUES (?, 'ok', 'url', ?, ?)
        """,
        (story_id, text, len(text)),
    )
    connection.commit()


def _case(person_name: str | None = "Ada Example") -> dict[str, str | None]:
    return {
        "person_name": person_name,
        "private_org": "Example Strategies",
        "private_time": "2024",
        "public_org": "Government of Canada",
        "public_time": "2021",
        "jurisdiction": "Canada",
    }


def _response(content: str) -> LLMResponse:
    return LLMResponse(
        content=content,
        request_id="request-1",
        usage={"prompt_tokens": 10, "completion_tokens": 5},
        raw={"id": "response-1"},
        structured_output="json_schema",
    )


def test_prompt_includes_dynamic_schema_titles_and_plain_delimiters():
    extraction = load_config().topics["revolving_door_ca"].extraction

    prompt = build_extraction_prompt(
        extraction,
        [PromptArticle("story-1", "Quoted title", "Stored article text")],
    )

    assert "OUTPUT SCHEMA" in prompt
    assert '"person_name"' in prompt
    assert 'title: "Quoted title"' in prompt
    assert "--- ARTICLE START ---" in prompt
    assert "<article" not in prompt


def test_packing_uses_token_ceiling_and_truncates_oversized_article():
    extraction = load_config().topics["revolving_door_ca"].extraction
    candidates = [
        Candidate("story-1", "One", "a" * 1800),
        Candidate("story-2", "Two", "b" * 1800),
    ]

    batches = pack_candidates(
        candidates,
        extraction=extraction,
        estimator=FakeEstimator(),
        max_input_tokens=2600,
    )

    assert [len(batch.articles) for batch in batches] == [1, 1]
    assert all(batch.estimated_tokens <= 2600 for batch in batches)
    assert len(batches[0].articles[0].text) < 1800


def test_extract_persists_relevant_and_omitted_articles_then_exports(tmp_path):
    config = load_config()
    connection = init_db(tmp_path / "pipeline.db")
    try:
        _seed_article(connection, "story-1", title="Lobby transition", text="Text one " * 100)
        _seed_article(connection, "story-2", title="Other story", text="Text two " * 100)
        client = FakeClient([{"results": [{"story_id": "story-1", "cases": [_case()]}]}])
        limiter = FakeLimiter()
        exports: list[int] = []
        progress_updates = []

        summary = extract_topic(
            config,
            "revolving_door_ca",
            connection,
            limit=2,
            credentials=LLMCredentials(base_url="https://llm.test/v1", api_key=SecretStr("x")),
            _client=client,
            _rate_limiter=limiter,
            _exporter=lambda: exports.append(1),
            progress=progress_updates.append,
        )

        rows = connection.execute(
            "SELECT story_id, relevant, validation_status FROM story_extractions ORDER BY story_id"
        ).fetchall()
        assert [tuple(row) for row in rows] == [
            ("story-1", 1, "valid"),
            ("story-2", 0, "valid"),
        ]
        assert connection.execute("SELECT COUNT(*) FROM cases").fetchone()[0] == 1
        assert summary.processed == summary.succeeded == 2
        assert summary.failed == 0
        assert limiter.calls == 1
        assert exports == [1]
        assert [
            (update.processed, update.total, update.batch, update.batch_count, update.status)
            for update in progress_updates
        ] == [(0, 2, 1, 1, "requesting"), (2, 2, 1, 1, "completed")]
    finally:
        connection.close()


def test_unknown_story_id_invalidates_entire_batch_without_cases(tmp_path):
    config = load_config()
    connection = init_db(tmp_path / "pipeline.db")
    try:
        _seed_article(connection, "story-1", title="Lobby transition", text="Text " * 100)
        client = FakeClient([{"results": [{"story_id": "unknown", "cases": [_case()]}]}] * 4)

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

        row = connection.execute(
            "SELECT relevant, validation_status FROM story_extractions"
        ).fetchone()
        assert tuple(row) == (None, "invalid")
        assert connection.execute("SELECT COUNT(*) FROM cases").fetchone()[0] == 0
        assert summary.failed == 1
    finally:
        connection.close()


def test_extract_dry_run_has_no_database_writes(tmp_path):
    config = load_config()
    connection = init_db(tmp_path / "pipeline.db")
    try:
        _seed_article(connection, "story-1", title="Lobby transition", text="Text " * 100)
        progress_updates = []

        summary = extract_topic(
            config,
            "revolving_door_ca",
            connection,
            limit=1,
            dry_run=True,
            progress=progress_updates.append,
        )

        assert summary.processed == summary.skipped == 1
        assert progress_updates == []
        assert connection.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM extraction_batches").fetchone()[0] == 0
    finally:
        connection.close()


def test_llm_config_defaults_to_three_retries_after_the_initial_request():
    config = load_config()
    values = config.llm.model_dump()
    values.pop("max_retries", None)

    assert LLMConfig.model_validate(values).max_retries == 3


def test_extract_retries_then_persists_one_successful_extraction(tmp_path):
    config = load_config()
    connection = init_db(tmp_path / "pipeline.db")
    try:
        _seed_article(connection, "story-1", title="Lobby transition", text="Text " * 100)
        client = FakeClient(
            [
                ExtractionError("temporary provider failure"),
                {"results": [{"story_id": "story-1", "cases": [_case()]}]},
            ]
        )
        limiter = FakeLimiter()

        summary = extract_topic(
            config,
            "revolving_door_ca",
            connection,
            limit=1,
            credentials=LLMCredentials(base_url="https://llm.test/v1", api_key=SecretStr("x")),
            _client=client,
            _rate_limiter=limiter,
            _exporter=lambda: None,
        )

        assert summary.processed == summary.succeeded == 1
        assert summary.failed == 0
        assert len(client.calls) == limiter.calls == 2
        assert connection.execute("SELECT COUNT(*) FROM story_extractions").fetchone()[0] == 1
        batch = connection.execute(
            "SELECT status, attempt_count FROM extraction_batches"
        ).fetchone()
        assert tuple(batch) == ("completed", 2)
    finally:
        connection.close()


@pytest.mark.parametrize("failure_kind", ["request", "parse", "schema"])
def test_extract_retries_all_failure_types_then_keeps_retryable_unparsed_row(
    tmp_path, failure_kind
):
    config = load_config()
    connection = init_db(tmp_path / "pipeline.db")
    try:
        _seed_article(connection, "story-1", title="Lobby transition", text="Text " * 100)
        if failure_kind == "request":
            failures: list[dict[str, object] | LLMResponse | Exception] = [
                ExtractionError("temporary provider failure")
            ] * 4
        elif failure_kind == "parse":
            failures = [_response("not valid JSON")] * 4
        else:
            failures = [{"results": [{"story_id": "unknown", "cases": [_case()]}]}] * 4
        client = FakeClient(failures)
        limiter = FakeLimiter()

        summary = extract_topic(
            config,
            "revolving_door_ca",
            connection,
            limit=1,
            credentials=LLMCredentials(base_url="https://llm.test/v1", api_key=SecretStr("x")),
            _client=client,
            _rate_limiter=limiter,
            _exporter=lambda: None,
        )

        assert summary.failed == 1
        assert len(client.calls) == limiter.calls == 4
        batch = connection.execute(
            "SELECT status, attempt_count FROM extraction_batches"
        ).fetchone()
        expected_status = "invalid" if failure_kind == "schema" else "unparsed"
        assert tuple(batch) == (expected_status, 4)
        extraction = connection.execute(
            "SELECT relevant, validation_status FROM story_extractions"
        ).fetchone()
        assert tuple(extraction) == (None, expected_status)

        recovered = extract_topic(
            config,
            "revolving_door_ca",
            connection,
            limit=1,
            credentials=LLMCredentials(base_url="https://llm.test/v1", api_key=SecretStr("x")),
            _client=FakeClient([{"results": []}]),
            _rate_limiter=FakeLimiter(),
            _exporter=lambda: None,
        )

        assert recovered.succeeded == 1
        assert connection.execute("SELECT COUNT(*) FROM story_extractions").fetchone()[0] == 1
        extraction = connection.execute(
            "SELECT relevant, validation_status FROM story_extractions"
        ).fetchone()
        assert tuple(extraction) == (0, "valid")
    finally:
        connection.close()


def test_candidate_selection_prioritizes_new_stories_and_excludes_valid_nonmatches(tmp_path):
    config = load_config()
    connection = init_db(tmp_path / "pipeline.db")
    try:
        for story_id in ("story-1", "story-2", "story-3", "story-4", "story-5"):
            _seed_article(connection, story_id, title="Transition", text="Text " * 100)
        connection.executemany(
            """
            INSERT INTO story_extractions(
                topic, story_id, prompt_version, relevant, validation_status
            )
            VALUES ('revolving_door_ca', ?, ?, ?, ?)
            """,
            [
                ("story-1", config.llm.prompt_version, None, "invalid"),
                ("story-2", config.llm.prompt_version, None, "unparsed"),
                ("story-5", config.llm.prompt_version, 0, "valid"),
            ],
        )
        connection.commit()

        candidates = _candidate_rows(
            connection,
            "revolving_door_ca",
            config.llm.prompt_version,
            config.topics["revolving_door_ca"].fetch_priority_title_terms,
            limit=None,
        )

        assert [candidate.story_id for candidate in candidates] == [
            "story-3",
            "story-4",
            "story-1",
            "story-2",
        ]
    finally:
        connection.close()


def test_manual_request_selects_one_bounded_batch_and_writes_prompt_only_artifact(
    tmp_path, monkeypatch
):
    import mc_pipeline.extract as extract_module

    config = load_config()
    config = config.model_copy(
        update={"llm": config.llm.model_copy(update={"max_input_tokens": 150})}
    )
    connection = init_db(tmp_path / "pipeline.db")
    monkeypatch.setattr(extract_module, "TokenEstimator", FakeManualRequestEstimator)
    monkeypatch.setenv("LLM_API_KEY", "manual-preview-secret")
    try:
        _seed_article(connection, "story-1", title="First title", text="First stored text")
        _seed_article(connection, "story-2", title="Second title", text="Second stored text")
        _seed_article(connection, "story-3", title="Third title", text="Third stored text")
        before = "\n".join(connection.iterdump())

        artifact = build_manual_request(
            config,
            "revolving_door_ca",
            connection,
            limit=2,
            batch_number=2,
        )
        output = tmp_path / "manual-request.json"

        assert write_manual_request(artifact, output) == output
        assert artifact["prompt_version"] == config.llm.prompt_version
        assert artifact["story_ids"] == ["story-2"]
        assert artifact["estimated_input_tokens"] == 116
        expected_user_prompt = build_extraction_prompt(
            config.topics["revolving_door_ca"].extraction,
            [PromptArticle("story-2", "Second title", "Second stored text")],
        )
        assert artifact["system_prompt"] == SYSTEM_PROMPT
        assert artifact["user_prompt"] == expected_user_prompt
        assert output.read_text(encoding="utf-8") == (
            "SYSTEM PROMPT\n=============\n\n"
            f"{SYSTEM_PROMPT}\n\n"
            "USER PROMPT\n===========\n\n"
            f"{expected_user_prompt}\n"
        )
        assert "manual-preview-secret" not in output.read_text(encoding="utf-8")
        assert '"messages"' not in output.read_text(encoding="utf-8")
        assert "\n".join(connection.iterdump()) == before
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("limit", "batch_number"),
    [(0, 1), (2, 0), (2, 3)],
)
def test_manual_request_rejects_invalid_limit_or_batch_number(
    tmp_path, monkeypatch, limit, batch_number
):
    import mc_pipeline.extract as extract_module

    config = load_config()
    config = config.model_copy(
        update={"llm": config.llm.model_copy(update={"max_input_tokens": 150})}
    )
    connection = init_db(tmp_path / "pipeline.db")
    monkeypatch.setattr(extract_module, "TokenEstimator", FakeManualRequestEstimator)
    try:
        _seed_article(connection, "story-1", title="First title", text="First stored text")
        _seed_article(connection, "story-2", title="Second title", text="Second stored text")

        with pytest.raises(ConfigError):
            build_manual_request(
                config,
                "revolving_door_ca",
                connection,
                limit=limit,
                batch_number=batch_number,
            )
    finally:
        connection.close()


def test_manual_request_reconstructs_exact_stored_batch(tmp_path):
    config = load_config()
    connection = init_db(tmp_path / "pipeline.db")
    try:
        request_record = {
            "system_prompt": "stored system prompt",
            "user_prompt": "stored user prompt with article text",
            "estimated_input_tokens": 321,
        }
        cursor = connection.execute(
            """
            INSERT INTO extraction_batches(
                topic, model, prompt_version, prompt_hash, schema_hash, status,
                story_ids_json, request_json
            )
            VALUES (?, ?, ?, 'prompt-hash', 'schema-hash', 'completed', ?, ?)
            """,
            (
                "revolving_door_ca",
                config.llm.model,
                config.llm.prompt_version,
                json.dumps(["story-1", "story-2"]),
                json.dumps(request_record),
            ),
        )
        connection.commit()

        artifact = build_manual_request(
            config,
            "revolving_door_ca",
            connection,
            extraction_batch_id=int(cursor.lastrowid),
        )

        assert artifact["source_extraction_batch_id"] == cursor.lastrowid
        assert artifact["story_ids"] == ["story-1", "story-2"]
        assert artifact["estimated_input_tokens"] == 321
        assert artifact["system_prompt"] == "stored system prompt"
        assert artifact["user_prompt"] == "stored user prompt with article text"
        output = tmp_path / "stored-prompts.txt"
        assert write_manual_request(artifact, output) == output
        assert output.read_text(encoding="utf-8") == (
            "SYSTEM PROMPT\n=============\n\n"
            "stored system prompt\n\n"
            "USER PROMPT\n===========\n\n"
            "stored user prompt with article text\n"
        )
    finally:
        connection.close()
