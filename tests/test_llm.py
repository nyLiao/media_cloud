from __future__ import annotations

from typing import Any

import pytest
from pydantic import SecretStr

import mc_pipeline.llm as llm
from mc_pipeline.config import LLMConfig, LLMCredentials
from mc_pipeline.errors import ExtractionError
from mc_pipeline.llm import OpenAIExtractionClient


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        payload: dict[str, Any] | None = None,
        *,
        headers: dict[str, str] | None = None,
        text: str = "",
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = text

    def json(self) -> dict[str, Any]:
        if self._payload is None:
            raise ValueError("response did not contain JSON")
        return self._payload


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self._responses = list(responses)
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def post(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append((url, kwargs))
        return self._responses.pop(0)


def _config() -> LLMConfig:
    return LLMConfig(
        model="test-model",
        timeout_s=300,
        max_requests_per_min=30,
        max_input_tokens=95_000,
        max_output_tokens=100,
        temperature=0,
        structured_output="json_schema",
        prompt_version="v2",
    )


def _completion_response() -> FakeResponse:
    return FakeResponse(
        200,
        {
            "id": "chatcmpl-1",
            "choices": [{"message": {"content": '{"results":[]}'}}],
            "usage": {"prompt_tokens": 10},
        },
        headers={"x-request-id": "provider-request-1"},
    )


def _client(
    monkeypatch: pytest.MonkeyPatch,
    responses: list[FakeResponse],
    before_request=lambda: None,
) -> tuple[OpenAIExtractionClient, FakeSession]:
    session = FakeSession(responses)
    monkeypatch.setattr(llm.requests, "Session", lambda: session)
    client = OpenAIExtractionClient(
        _config(),
        LLMCredentials(
            base_url="https://provider.test/v1/",
            api_key=SecretStr("test-key"),
        ),
        before_request=before_request,
    )
    return client, session


def test_json_schema_request_is_normalized_through_session_post(monkeypatch):
    client, session = _client(monkeypatch, [_completion_response()])
    schema = {"type": "object", "properties": {"results": {"type": "array"}}}

    response = client.extract(system_prompt="system", user_prompt="user", schema=schema)

    assert response.content == '{"results":[]}'
    assert response.request_id == "provider-request-1"
    assert response.usage == {"prompt_tokens": 10}
    assert response.raw["id"] == "chatcmpl-1"
    assert response.structured_output == "json_schema"
    assert session.calls == [
        (
            "https://provider.test/v1/chat/completions",
            {
                "headers": {
                    "Authorization": "Bearer test-key",
                    "Content-Type": "application/json",
                },
                "json": {
                    "model": "test-model",
                    "messages": [
                        {"role": "system", "content": "system"},
                        {"role": "user", "content": "user"},
                    ],
                    "response_format": {
                        "type": "json_schema",
                        "json_schema": {
                            "name": "extraction_response",
                            "strict": True,
                            "schema": schema,
                        },
                    },
                    "temperature": 0,
                    "max_tokens": 100,
                },
                "timeout": 300,
            },
        )
    ]


def test_explicit_unsupported_schema_falls_back_once_and_rate_limits_both_calls(monkeypatch):
    paced: list[int] = []
    client, session = _client(
        monkeypatch,
        [
            FakeResponse(
                400,
                {"error": {"message": "response_format json_schema is unsupported"}},
            ),
            _completion_response(),
        ],
        before_request=lambda: paced.append(1),
    )

    response = client.extract(system_prompt="system", user_prompt="user", schema={"type": "object"})

    assert response.structured_output == "json_object"
    assert [call[1]["json"]["response_format"] for call in session.calls] == [
        {
            "type": "json_schema",
            "json_schema": {
                "name": "extraction_response",
                "strict": True,
                "schema": {"type": "object"},
            },
        },
        {"type": "json_object"},
    ]
    assert paced == [1, 1]


def test_unrelated_bad_request_does_not_fallback(monkeypatch):
    client, session = _client(
        monkeypatch,
        [FakeResponse(400, {"error": {"message": "invalid model name"}})],
    )

    with pytest.raises(ExtractionError, match="invalid model name"):
        client.extract(system_prompt="system", user_prompt="user", schema={"type": "object"})

    assert len(session.calls) == 1


def test_http_error_includes_status_request_id_and_bounded_response_body(monkeypatch):
    body = "provider failure: " + "x" * 10_000 + " end-of-body"
    client, session = _client(
        monkeypatch,
        [
            FakeResponse(
                502,
                headers={"x-request-id": "provider-request-502"},
                text=body,
            )
        ],
    )

    with pytest.raises(ExtractionError) as exc_info:
        client.extract(system_prompt="system", user_prompt="user", schema={"type": "object"})

    message = str(exc_info.value)
    assert "502" in message
    assert "provider-request-502" in message
    assert "provider failure:" in message
    assert "end-of-body" not in message
    assert len(message) < 2_000
    assert len(session.calls) == 1
