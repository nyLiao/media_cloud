"""Direct HTTP adapter for OpenAI-compatible extraction requests."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import requests

from .config import LLMConfig, LLMCredentials
from .errors import ExtractionError

MAX_ERROR_BODY_CHARS = 1000


@dataclass(frozen=True)
class LLMResponse:
    """Normalized provider response without authorization or header data."""

    content: str
    request_id: str | None
    usage: dict[str, Any] | None
    raw: dict[str, Any]
    structured_output: str


class LLMHTTPError(RuntimeError):
    """Bounded provider failure used for capability fallback and audit output."""

    def __init__(self, *, status_code: int, request_id: str | None, body: str) -> None:
        self.status_code = status_code
        self.request_id = request_id
        self.body = " ".join(body.split())[:MAX_ERROR_BODY_CHARS]
        request_detail = f" request_id={request_id}" if request_id else ""
        super().__init__(f"HTTP {status_code}{request_detail}: {self.body or 'empty response'}")


def _error_body(response: requests.Response) -> str:
    if response.text.strip():
        return response.text
    try:
        payload = response.json()
    except ValueError:
        return ""
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _unsupported_structured_output(error: BaseException) -> bool:
    status_code = getattr(error, "status_code", None)
    message = str(error).casefold()
    return status_code == 400 and any(
        marker in message
        for marker in ("json_schema", "response_format", "structured output", "unsupported format")
    )


def build_response_format(schema: dict[str, Any], mode: str) -> dict[str, Any]:
    """Return the OpenAI-compatible response-format payload for one request."""
    if mode == "json_schema":
        return {
            "type": "json_schema",
            "json_schema": {
                "name": "extraction_response",
                "strict": True,
                "schema": schema,
            },
        }
    return {"type": "json_object"}


class OpenAIExtractionClient:
    """Issue one serial attempt to an OpenAI-compatible HTTP endpoint."""

    def __init__(
        self,
        config: LLMConfig,
        credentials: LLMCredentials,
        *,
        before_request: Callable[[], None] | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self._config = config
        self._before_request = before_request
        self._base_url = credentials.base_url.rstrip("/")
        self._api_key = credentials.api_key.get_secret_value()
        self._session = session or requests.Session()

    def extract(
        self, *, system_prompt: str, user_prompt: str, schema: dict[str, Any]
    ) -> LLMResponse:
        """Return one provider response, downgrading only explicit schema rejection."""
        mode = self._config.structured_output
        try:
            response = self._create(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                schema=schema,
                mode=mode,
            )
        except Exception as exc:
            if mode != "json_schema" or not _unsupported_structured_output(exc):
                raise ExtractionError(f"LLM request failed: {exc}") from exc
            mode = "json_object"
            try:
                response = self._create(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    schema=schema,
                    mode=mode,
                )
            except Exception as fallback_exc:
                raise ExtractionError(
                    f"LLM fallback request failed: {fallback_exc}"
                ) from fallback_exc

        try:
            payload = response.json()
        except requests.JSONDecodeError as exc:
            raise ExtractionError(
                f"LLM response was not valid JSON: HTTP {response.status_code}"
            ) from exc
        if not isinstance(payload, dict):
            raise ExtractionError("LLM response JSON was not an object.")
        try:
            content = payload["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise ExtractionError(
                "LLM response did not contain choices[0].message.content."
            ) from exc
        if not isinstance(content, str) or not content.strip():
            raise ExtractionError("LLM response contained no JSON content.")
        usage_value = payload.get("usage")
        usage = usage_value if isinstance(usage_value, dict) else None
        return LLMResponse(
            content=content,
            request_id=response.headers.get("x-request-id"),
            usage=usage,
            raw=payload,
            structured_output=mode,
        )

    def _create(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        schema: dict[str, Any],
        mode: str,
    ) -> requests.Response:
        if self._before_request is not None:
            self._before_request()
        response = self._session.post(
            f"{self._base_url}/chat/completions",
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": self._config.model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "response_format": build_response_format(schema, mode),
                "temperature": self._config.temperature,
                "max_tokens": self._config.max_output_tokens,
            },
            timeout=self._config.timeout_s,
        )
        if response.status_code >= 400:
            raise LLMHTTPError(
                status_code=response.status_code,
                request_id=response.headers.get("x-request-id"),
                body=_error_body(response),
            )
        return response


def parse_json_content(content: str) -> Any:
    """Parse provider content while preserving a concise typed failure."""
    try:
        return json.loads(content)
    except json.JSONDecodeError as exc:
        raise ExtractionError(f"LLM response was not valid JSON: {exc}") from exc
