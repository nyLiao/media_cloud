"""Stage 4: two-stage, serial structured extraction over stored article text."""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol, cast

import tiktoken
from pydantic import ValidationError

from .config import AppConfig, LLMCredentials, TopicConfig
from .contracts import (
    build_response_json_schema,
    build_response_model,
    build_screening_json_schema,
    build_screening_model,
)
from .db import complete_pipeline_run, start_pipeline_run, transaction
from .errors import ConfigError, ExtractionError
from .identity import build_case_id, canonical_json, sha256_hex
from .llm import LLMResponse, OpenAIExtractionClient, parse_json_content
from .prompt import (
    EXTRACTION_SYSTEM_PROMPT,
    SCREENING_SYSTEM_PROMPT,
    USER_TEMPLATE_VERSION,
    PromptArticle,
    build_extraction_prompt,
    build_screening_prompt,
)
from .ratelimit import TokenBucket
from .stage import StageSummary, git_revision, package_version

ExportCallback = Callable[[], StageSummary]
Phase = Literal["screening", "extraction"]


class ExtractionClient(Protocol):
    """Minimal provider interface used by the extraction service."""

    def extract(
        self, *, system_prompt: str, user_prompt: str, schema: dict[str, Any]
    ) -> LLMResponse:
        """Return one structured response."""


@dataclass(frozen=True)
class Candidate:
    story_id: str
    title: str
    text: str
    publication_year: int | None = None


@dataclass(frozen=True)
class PackedBatch:
    articles: tuple[PromptArticle, ...]
    user_prompt: str
    estimated_tokens: int
    phase: Phase


@dataclass(frozen=True)
class ExtractionProgress:
    """One observable Stage 4 batch lifecycle update."""

    processed: int
    total: int
    batch: int
    batch_count: int
    status: str
    succeeded: int
    failed: int


ExtractionProgressReporter = Callable[[ExtractionProgress], None]
ExtractionDetailReporter = Callable[[str], None]


def _phase_prompt(phase: Phase, topic: TopicConfig, articles: Sequence[PromptArticle]) -> str:
    if phase == "screening":
        return build_screening_prompt(topic, articles)
    return build_extraction_prompt(topic, articles)


def _phase_system_prompt(phase: Phase) -> str:
    return SCREENING_SYSTEM_PROMPT if phase == "screening" else EXTRACTION_SYSTEM_PROMPT


def _phase_schema(phase: Phase, extraction: Any) -> dict[str, Any]:
    if phase == "screening":
        return build_screening_json_schema(extraction)
    return build_response_json_schema(extraction)


def _phase_prompt_version(config: AppConfig, phase: Phase) -> str:
    if phase == "screening":
        return config.llm.screening_prompt_version
    return config.llm.prompt_version


def _phase_input_tokens(config: AppConfig, phase: Phase) -> int:
    if phase == "screening":
        return config.llm.screening_input_tokens
    return config.llm.extraction_input_tokens


def build_manual_request(
    config: AppConfig,
    topic_name: str,
    connection: sqlite3.Connection,
    *,
    limit: int | None = None,
    batch_number: int = 1,
    extraction_batch_id: int | None = None,
    phase: Phase = "screening",
) -> dict[str, Any]:
    """Build one exact secret-free pair of prompts for manual LLM verification."""
    if topic_name not in config.topics:
        raise ConfigError(f"Unknown topic: {topic_name}")
    if batch_number < 1:
        raise ConfigError("batch_number must be at least 1")
    if extraction_batch_id is not None:
        if limit is not None or batch_number != 1:
            raise ConfigError("--batch-id cannot be combined with --limit or --batch")
        stored = connection.execute(
            """
            SELECT id, topic, prompt_version, story_ids_json, request_json
            FROM extraction_batches WHERE id = ?
            """,
            (extraction_batch_id,),
        ).fetchone()
        if stored is None or str(stored["topic"]) != topic_name:
            raise ConfigError(
                f"Extraction batch {extraction_batch_id} does not exist for topic {topic_name}"
            )
        try:
            request_record = json.loads(str(stored["request_json"]))
            story_ids = json.loads(str(stored["story_ids_json"]))
            system_prompt = str(request_record["system_prompt"])
            user_prompt = str(request_record["user_prompt"])
            estimated_tokens = int(request_record["estimated_input_tokens"])
            stored_phase = str(request_record.get("phase", "extraction"))
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ConfigError(
                f"Extraction batch {extraction_batch_id} has invalid stored request provenance"
            ) from exc
        return {
            "topic": topic_name,
            "phase": stored_phase,
            "prompt_version": str(stored["prompt_version"]),
            "source_extraction_batch_id": extraction_batch_id,
            "story_ids": story_ids,
            "estimated_input_tokens": estimated_tokens,
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
        }
    if limit is None or limit < 1:
        raise ConfigError("--limit is required when --batch-id is not provided")
    candidates = _candidates_for_phase(
        connection,
        topic_name,
        _phase_prompt_version(config, phase),
        config.topics[topic_name].fetch_priority_title_terms,
        limit,
        phase,
        screening_prompt_version=config.llm.screening_prompt_version,
    )
    batches = pack_candidates(
        candidates,
        topic=config.topics[topic_name],
        estimator=TokenEstimator(config.llm.model, allow_tokenizer_load=False),
        max_input_tokens=_phase_input_tokens(config, phase),
        phase=phase,
        max_articles=(
            config.llm.extraction_max_articles_per_batch if phase == "extraction" else None
        ),
    )
    if not batches:
        raise ConfigError(f"No pending {phase} candidates for topic: {topic_name}")
    if batch_number > len(batches):
        raise ConfigError(
            f"Batch {batch_number} does not exist; dry run produced {len(batches)} batches"
        )
    batch = batches[batch_number - 1]
    return {
        "topic": topic_name,
        "phase": phase,
        "prompt_version": _phase_prompt_version(config, phase),
        "batch_number": batch_number,
        "batch_count": len(batches),
        "story_ids": [article.story_id for article in batch.articles],
        "estimated_input_tokens": batch.estimated_tokens,
        "system_prompt": _phase_system_prompt(phase),
        "user_prompt": batch.user_prompt,
    }


def write_manual_request(artifact: dict[str, Any], output: Path) -> Path:
    """Atomically write one human-readable system and user prompt pair."""
    output.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=output.parent, prefix=f".{output.name}.", suffix=".tmp"
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write("SYSTEM PROMPT\n=============\n\n")
            handle.write(str(artifact["system_prompt"]))
            handle.write("\n\nUSER PROMPT\n===========\n\n")
            handle.write(str(artifact["user_prompt"]))
            handle.write("\n")
        os.replace(temporary, output)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return output


class TokenEstimator:
    """Estimate and truncate prompts without requiring tokenizer downloads."""

    def __init__(self, model: str, *, allow_tokenizer_load: bool = True) -> None:
        self._encoding: Any | None
        if not allow_tokenizer_load:
            self._encoding = None
            return
        try:
            self._encoding = tiktoken.encoding_for_model(model)
        except Exception:
            self._encoding = None

    def count(self, text: str) -> int:
        if self._encoding is not None:
            return len(self._encoding.encode(text))
        return max(1, (len(text.encode("utf-8")) + 1) // 2)

    def truncate(self, text: str, max_tokens: int) -> str:
        if max_tokens <= 0:
            return ""
        if self._encoding is not None:
            return str(self._encoding.decode(self._encoding.encode(text)[:max_tokens]))
        return text.encode("utf-8")[: max_tokens * 2].decode("utf-8", errors="ignore")


def _candidates_for_phase(
    connection: sqlite3.Connection,
    topic_name: str,
    prompt_version: str,
    priority_terms: Sequence[str],
    limit: int | None,
    phase: Phase,
    *,
    screening_prompt_version: str | None = None,
) -> list[Candidate]:
    priority_sql = "0"
    priority_parameters: list[Any] = []
    if priority_terms:
        clauses = []
        for term in priority_terms:
            clauses.append("LOWER(COALESCE(s.title, '')) LIKE ?")
            priority_parameters.append(f"%{term.casefold()}%")
        priority_sql = f"CASE WHEN {' OR '.join(clauses)} THEN 0 ELSE 1 END"
    if phase == "screening":
        join = """
            LEFT JOIN story_extractions AS ss
              ON ss.topic = sts.topic AND ss.story_id = sts.story_id AND ss.prompt_version = ?
        """
        pending = "(ss.id IS NULL OR ss.validation_status IN ('invalid', 'unparsed'))"
        order_status = "CASE WHEN ss.id IS NULL THEN 0 ELSE 1 END, ss.extracted_at"
    else:
        screening_version = screening_prompt_version or prompt_version
        join = """
            JOIN story_extractions AS ss
              ON ss.topic = sts.topic AND ss.story_id = sts.story_id AND ss.prompt_version = ?
            LEFT JOIN story_extractions AS se
              ON se.topic = sts.topic AND se.story_id = sts.story_id AND se.prompt_version = ?
        """
        pending = (
            "ss.validation_status = 'valid' AND ss.relevant = 1 "
            "AND (se.id IS NULL OR se.validation_status IN ('invalid', 'unparsed'))"
        )
        order_status = "CASE WHEN se.id IS NULL THEN 0 ELSE 1 END, se.extracted_at"
    limit_sql = " LIMIT ?" if limit is not None else ""
    parameters: list[Any] = [prompt_version if phase == "screening" else screening_version]
    if phase == "extraction":
        parameters.append(prompt_version)
    parameters.extend([topic_name, *priority_parameters])
    if limit is not None:
        parameters.append(limit)
    rows = connection.execute(
        f"""
        SELECT s.story_id, COALESCE(s.title, '') AS title, a.text,
               CASE WHEN substr(s.publish_date, 1, 4) GLOB '[0-9][0-9][0-9][0-9]'
                    THEN CAST(substr(s.publish_date, 1, 4) AS INTEGER) END AS publication_year
        FROM story_topic_state AS sts
        JOIN stories AS s ON s.story_id = sts.story_id
        JOIN articles AS a ON a.story_id = s.story_id
        {join}
        WHERE sts.topic = ? AND sts.qc_status = 'ok' AND a.fetch_status = 'ok'
          AND a.text IS NOT NULL AND LENGTH(TRIM(a.text)) > 0 AND {pending}
        ORDER BY {order_status}, {priority_sql}, s.publish_date, s.story_id
        {limit_sql}
        """,
        parameters,
    ).fetchall()
    return [
        Candidate(
            str(row["story_id"]), str(row["title"]), str(row["text"]), row["publication_year"]
        )
        for row in rows
    ]


def _candidate_rows(
    connection: sqlite3.Connection,
    topic_name: str,
    prompt_version: str,
    priority_terms: Sequence[str],
    limit: int | None,
) -> list[Candidate]:
    """Compatibility selector for callers that need pending Stage 1 candidates."""
    return _candidates_for_phase(
        connection, topic_name, prompt_version, priority_terms, limit, "screening"
    )


def _estimate_prompt(estimator: TokenEstimator, system_prompt: str, user_prompt: str) -> int:
    return estimator.count(system_prompt) + estimator.count(user_prompt) + 16


def pack_candidates(
    candidates: Sequence[Candidate],
    *,
    topic: TopicConfig,
    estimator: TokenEstimator,
    max_input_tokens: int,
    phase: Phase = "extraction",
    max_articles: int | None = 10,
) -> list[PackedBatch]:
    """Greedily pack candidates under the token cap and Stage 2 article cap."""
    batches: list[PackedBatch] = []
    current: list[PromptArticle] = []
    system_prompt = _phase_system_prompt(phase)
    article_limit = max_articles if phase == "extraction" else None
    for candidate in candidates:
        article = PromptArticle(
            candidate.story_id, candidate.title, candidate.text, candidate.publication_year
        )
        proposed = (*current, article)
        proposed_prompt = _phase_prompt(phase, topic, proposed)
        proposed_tokens = _estimate_prompt(estimator, system_prompt, proposed_prompt)
        if current and (
            proposed_tokens > max_input_tokens
            or (article_limit is not None and len(proposed) > article_limit)
        ):
            prompt = _phase_prompt(phase, topic, current)
            batches.append(
                PackedBatch(
                    tuple(current),
                    prompt,
                    _estimate_prompt(estimator, system_prompt, prompt),
                    phase,
                )
            )
            current = []
            proposed = (article,)
            proposed_prompt = _phase_prompt(phase, topic, proposed)
            proposed_tokens = _estimate_prompt(estimator, system_prompt, proposed_prompt)
        if proposed_tokens > max_input_tokens:
            empty_prompt = _phase_prompt(
                phase,
                topic,
                (
                    PromptArticle(
                        candidate.story_id, candidate.title, "", candidate.publication_year
                    ),
                ),
            )
            available = max_input_tokens - _estimate_prompt(estimator, system_prompt, empty_prompt)
            if available <= 0:
                raise ConfigError(
                    "max_input_tokens is too small for the phase schema and article labels"
                )
            article = PromptArticle(
                candidate.story_id,
                candidate.title,
                estimator.truncate(candidate.text, available),
                candidate.publication_year,
            )
        current.append(article)
    if current:
        prompt = _phase_prompt(phase, topic, current)
        batches.append(
            PackedBatch(
                tuple(current), prompt, _estimate_prompt(estimator, system_prompt, prompt), phase
            )
        )
    if any(batch.estimated_tokens > max_input_tokens for batch in batches):
        raise ConfigError("packed extraction batch exceeds max_input_tokens")
    return batches


def _insert_batch(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    topic_name: str,
    config: AppConfig,
    prompt_hash: str,
    schema_hash: str,
    batch: PackedBatch,
    credentials: LLMCredentials,
) -> int:
    request_json = canonical_json(
        {
            "phase": batch.phase,
            "system_prompt": _phase_system_prompt(batch.phase),
            "user_prompt": batch.user_prompt,
            "estimated_input_tokens": batch.estimated_tokens,
        }
    )
    with transaction(connection):
        cursor = connection.execute(
            """
            INSERT INTO extraction_batches(
                pipeline_run_id, topic, model, base_url, prompt_version, prompt_hash, schema_hash,
                status, story_ids_json, attempt_count, request_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, 0, ?)
            """,
            (
                run_id,
                topic_name,
                config.llm.model,
                credentials.base_url,
                _phase_prompt_version(config, batch.phase),
                prompt_hash,
                schema_hash,
                canonical_json([article.story_id for article in batch.articles]),
                request_json,
            ),
        )
        if cursor.lastrowid is None:
            raise ExtractionError("Failed to create extraction batch.")
        return int(cursor.lastrowid)


def _response_raw_json(response: LLMResponse) -> str:
    return canonical_json(
        {
            "provider_response": response.raw,
            "request_id": response.request_id,
            "structured_output": response.structured_output,
        }
    )


def _persist_phase_failure(
    connection: sqlite3.Connection,
    *,
    phase: Phase,
    batch_id: int,
    topic_name: str,
    story_ids: Sequence[str],
    config: AppConfig,
    validation_status: str,
    error: str,
    attempt_count: int,
    response: LLMResponse | None,
) -> None:
    now = datetime.now(UTC).isoformat()
    with transaction(connection):
        connection.executemany(
            """
            INSERT INTO story_extractions(
                topic, story_id, extraction_batch_id, model, prompt_version, relevant,
                reject_reason, validation_status, validation_error, usage_json, raw_json,
                extracted_at
            ) VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?)
            ON CONFLICT(topic, story_id, prompt_version) DO UPDATE SET
                extraction_batch_id=excluded.extraction_batch_id, model=excluded.model,
                relevant=NULL,
                reject_reason=NULL, validation_status=excluded.validation_status,
                validation_error=excluded.validation_error, payload_json=NULL,
                usage_json=excluded.usage_json, raw_json=excluded.raw_json,
                extracted_at=excluded.extracted_at
            """,
            [
                (
                    topic_name,
                    story_id,
                    batch_id,
                    config.llm.model,
                    _phase_prompt_version(config, phase),
                    validation_status,
                    error[:1000],
                    canonical_json(response.usage) if response and response.usage else None,
                    _response_raw_json(response) if response else None,
                    now,
                )
                for story_id in story_ids
            ],
        )
        connection.execute(
            """
            UPDATE extraction_batches SET status=?, attempt_count=?, response_json=?, usage_json=?,
                raw_json=?, completed_at=CURRENT_TIMESTAMP, error=? WHERE id=?
            """,
            (
                validation_status,
                attempt_count,
                response.content if response else None,
                canonical_json(response.usage) if response and response.usage else None,
                _response_raw_json(response) if response else None,
                error[:1000],
                batch_id,
            ),
        )


def _validate_expected_ids(returned_ids: Sequence[str], expected_ids: Sequence[str]) -> None:
    if len(returned_ids) != len(set(returned_ids)) or set(returned_ids) != set(expected_ids):
        raise ValueError("response must contain every expected story_id exactly once")


def _validate_returned_extraction_ids(
    returned_ids: Sequence[str], expected_ids: Sequence[str]
) -> None:
    if len(returned_ids) != len(set(returned_ids)) or not set(returned_ids).issubset(expected_ids):
        raise ValueError("extraction response must contain each supplied story_id at most once")


def _persist_screening_response(
    connection: sqlite3.Connection,
    *,
    batch_id: int,
    topic_name: str,
    expected_ids: Sequence[str],
    config: AppConfig,
    payload: Any,
    response: LLMResponse,
    attempt_count: int,
) -> tuple[int, int]:
    validated = build_screening_model(config.topics[topic_name].extraction).model_validate(payload)
    decisions = cast(Any, validated).decisions
    _validate_expected_ids([str(decision.story_id) for decision in decisions], expected_ids)
    by_id = {str(decision.story_id): decision for decision in decisions}
    now = datetime.now(UTC).isoformat()
    with transaction(connection):
        for story_id in expected_ids:
            decision = by_id[story_id]
            connection.execute(
                """
                INSERT INTO story_extractions(
                    topic, story_id, extraction_batch_id, model, prompt_version, relevant,
                    validation_status, validation_error, payload_json, usage_json, raw_json,
                    extracted_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'valid', NULL, ?, ?, ?, ?)
                ON CONFLICT(topic, story_id, prompt_version) DO UPDATE SET
                    extraction_batch_id=excluded.extraction_batch_id, model=excluded.model,
                    relevant=excluded.relevant, reject_reason=NULL, validation_status='valid',
                    validation_error=NULL, payload_json=excluded.payload_json,
                    usage_json=excluded.usage_json, raw_json=excluded.raw_json,
                    extracted_at=excluded.extracted_at
                """,
                (
                    topic_name,
                    story_id,
                    batch_id,
                    config.llm.model,
                    config.llm.screening_prompt_version,
                    int(decision.relevant),
                    canonical_json(decision.model_dump(mode="json")),
                    canonical_json(response.usage) if response.usage else None,
                    _response_raw_json(response),
                    now,
                ),
            )
        connection.execute(
            """
            UPDATE extraction_batches SET status='completed', attempt_count=?, response_json=?,
                usage_json=?, raw_json=?, completed_at=CURRENT_TIMESTAMP, error=NULL WHERE id=?
            """,
            (
                attempt_count,
                canonical_json(payload),
                canonical_json(response.usage) if response.usage else None,
                _response_raw_json(response),
                batch_id,
            ),
        )
    return len(expected_ids), 0


def _persist_extraction_response(
    connection: sqlite3.Connection,
    *,
    batch_id: int,
    topic_name: str,
    expected_ids: Sequence[str],
    config: AppConfig,
    payload: Any,
    response: LLMResponse,
    attempt_count: int,
) -> tuple[int, int]:
    validated = build_response_model(config.topics[topic_name].extraction).model_validate(payload)
    results = cast(Any, validated).results
    _validate_returned_extraction_ids([str(result.story_id) for result in results], expected_ids)
    by_id = {str(result.story_id): result for result in results}
    placeholders = ",".join("?" for _ in expected_ids)
    rows = connection.execute(
        f"""
        SELECT story_id, media_name, url,
               publish_date
        FROM stories WHERE story_id IN ({placeholders})
        """,
        tuple(expected_ids),
    ).fetchall()
    provenance = {str(row["story_id"]): row for row in rows}
    now = datetime.now(UTC).isoformat()
    case_count = 0
    with transaction(connection):
        for story_id in expected_ids:
            result = by_id.get(story_id)
            relevant = result is not None
            payload_json = canonical_json(
                result.model_dump(mode="json")
                if result is not None
                else {"story_id": story_id, "cases": [], "omitted_from_response": True}
            )
            screening_row = connection.execute(
                """
                SELECT id FROM story_extractions
                WHERE topic = ? AND story_id = ? AND prompt_version = ?
                  AND validation_status = 'valid' AND relevant = 1
                """,
                (topic_name, story_id, config.llm.screening_prompt_version),
            ).fetchone()
            if screening_row is None:
                raise ExtractionError(f"Missing valid screening decision for {story_id}.")
            connection.execute(
                """
                INSERT INTO story_extractions(
                    topic, story_id, extraction_batch_id, model, prompt_version, relevant,
                    reject_reason, validation_status, validation_error, payload_json, usage_json,
                    raw_json, extracted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'valid', NULL, ?, ?, ?, ?)
                ON CONFLICT(topic, story_id, prompt_version) DO UPDATE SET
                    extraction_batch_id=excluded.extraction_batch_id, model=excluded.model,
                    relevant=excluded.relevant, reject_reason=excluded.reject_reason,
                    validation_status='valid', validation_error=NULL,
                    payload_json=excluded.payload_json,
                    usage_json=excluded.usage_json, raw_json=excluded.raw_json,
                    extracted_at=excluded.extracted_at
                """,
                (
                    topic_name,
                    story_id,
                    batch_id,
                    config.llm.model,
                    config.llm.prompt_version,
                    int(relevant),
                    None,
                    payload_json,
                    canonical_json(response.usage) if response.usage else None,
                    _response_raw_json(response),
                    now,
                ),
            )
            extraction_row = connection.execute(
                (
                    "SELECT id FROM story_extractions "
                    "WHERE topic=? AND story_id=? AND prompt_version=?"
                ),
                (topic_name, story_id, config.llm.prompt_version),
            ).fetchone()
            if extraction_row is None:
                raise ExtractionError(f"Failed to persist extraction for {story_id}.")
            extraction_id = int(extraction_row["id"])
            connection.execute("DELETE FROM cases WHERE story_extraction_id=?", (extraction_id,))
            if result is None:
                continue
            for ordinal, case in enumerate(result.cases):
                values = case.model_dump(mode="json")
                story = provenance[story_id]
                connection.execute(
                    """
                    INSERT INTO cases(
                        case_id, story_extraction_id, case_type, person_name,
                        link_category, previous_org, previous_sector, current_org, current_sector,
                        jurisdiction, expected_resolvable, status, source_outlet, source_url, claim,
                        evidence_verified, raw_json, private_org, private_time, public_org,
                        public_time
                    ) VALUES (
                        ?, ?, 'individual', ?, ?, ?, 'private', ?, 'public', ?, 'yes', 'accepted',
                        ?, ?, ?, 0, ?, ?, ?, ?, ?
                    )
                    """,
                    (
                        build_case_id(
                            topic=topic_name,
                            story_id=story_id,
                            prompt_version=config.llm.prompt_version,
                            ordinal=ordinal,
                        ),
                        extraction_id,
                        values.get("person_name"),
                        config.topics[topic_name].link_category,
                        values.get("private_org"),
                        values.get("public_org"),
                        values.get("jurisdiction"),
                        story["media_name"],
                        story["url"],
                        canonical_json(values),
                        canonical_json(values),
                        values.get("private_org"),
                        values.get("private_time"),
                        values.get("public_org"),
                        values.get("public_time"),
                    ),
                )
                case_count += 1
        connection.execute(
            """
            UPDATE extraction_batches SET status='completed', attempt_count=?, response_json=?,
                usage_json=?, raw_json=?, completed_at=CURRENT_TIMESTAMP, error=NULL WHERE id=?
            """,
            (
                attempt_count,
                canonical_json(payload),
                canonical_json(response.usage) if response.usage else None,
                _response_raw_json(response),
                batch_id,
            ),
        )
    return len(expected_ids), case_count


def _provenance(config: AppConfig, topic_name: str) -> tuple[str, str, str, str]:
    relevant = {
        "llm": config.llm.model_dump(mode="json"),
        "topic": config.topics[topic_name].model_dump(mode="json"),
    }
    return (
        sha256_hex(relevant),
        canonical_json(relevant),
        git_revision(),
        canonical_json(
            {
                "requests": package_version("requests"),
                "tiktoken": package_version("tiktoken"),
                "media-cloud-pipeline": package_version("media-cloud-pipeline"),
            }
        ),
    )


def _planned_details(batches: Sequence[PackedBatch]) -> tuple[str, ...]:
    return tuple(
        canonical_json(
            {
                "endpoint": "llm_extract",
                "phase": batch.phase,
                "batch": index,
                "articles": len(batch.articles),
                "estimated_input_tokens": batch.estimated_tokens,
                "story_ids": [article.story_id for article in batch.articles],
            }
        )
        for index, batch in enumerate(batches, start=1)
    )


def _run_phase(
    connection: sqlite3.Connection,
    *,
    phase: Phase,
    batches: Sequence[PackedBatch],
    topic_name: str,
    config: AppConfig,
    credentials: LLMCredentials,
    client: ExtractionClient,
    limiter: TokenBucket,
    run_id: int,
    progress: ExtractionProgressReporter | None,
    detail_reporter: ExtractionDetailReporter | None,
    total: int,
    completed_before: int,
    exporter: ExportCallback,
) -> tuple[int, int, int, list[str]]:
    extraction = config.topics[topic_name].extraction
    schema = _phase_schema(phase, extraction)
    system_prompt = _phase_system_prompt(phase)
    prompt_hash = sha256_hex(
        {
            "phase": phase,
            "system": system_prompt,
            "user_template": USER_TEMPLATE_VERSION,
            "schema": schema,
        }
    )
    schema_hash = sha256_hex(schema)
    succeeded = failed = accepted_cases = 0
    details: list[str] = []
    persist = _persist_screening_response if phase == "screening" else _persist_extraction_response
    for batch_index, batch in enumerate(batches, start=1):
        batch_id = _insert_batch(
            connection,
            run_id=run_id,
            topic_name=topic_name,
            config=config,
            prompt_hash=prompt_hash,
            schema_hash=schema_hash,
            batch=batch,
            credentials=credentials,
        )
        story_ids = [article.story_id for article in batch.articles]
        for attempt_number in range(1, config.llm.max_retries + 2):
            if progress is not None:
                progress(
                    ExtractionProgress(
                        completed_before + succeeded + failed,
                        total,
                        batch_index,
                        len(batches),
                        f"{phase}: requesting",
                        succeeded,
                        failed,
                    )
                )
            if client is not None:
                limiter.acquire()
            response: LLMResponse | None = None
            try:
                response = client.extract(
                    system_prompt=system_prompt, user_prompt=batch.user_prompt, schema=schema
                )
                processed, case_count = persist(
                    connection,
                    batch_id=batch_id,
                    topic_name=topic_name,
                    expected_ids=story_ids,
                    config=config,
                    payload=parse_json_content(response.content),
                    response=response,
                    attempt_count=attempt_number,
                )
            except (ExtractionError, ValidationError, ValueError) as exc:
                detail = canonical_json(
                    {
                        "event": "llm_attempt_failed",
                        "phase": phase,
                        "batch": batch_index,
                        "batch_id": batch_id,
                        "attempt": attempt_number,
                        "error": str(exc)[:1000],
                    }
                )
                details.append(detail)
                if detail_reporter is not None:
                    detail_reporter(detail)
                if attempt_number <= config.llm.max_retries:
                    continue
                _persist_phase_failure(
                    connection,
                    phase=phase,
                    batch_id=batch_id,
                    topic_name=topic_name,
                    story_ids=story_ids,
                    config=config,
                    validation_status="invalid"
                    if isinstance(exc, (ValidationError, ValueError))
                    else "unparsed",
                    error=str(exc),
                    attempt_count=attempt_number,
                    response=response,
                )
                failed += len(story_ids)
                exporter()
                detail = canonical_json(
                    {
                        "event": "llm_batch_failed",
                        "phase": phase,
                        "batch": batch_index,
                        "batch_id": batch_id,
                        "articles": len(story_ids),
                        "attempts": attempt_number,
                    }
                )
                details.append(detail)
                if detail_reporter is not None:
                    detail_reporter(detail)
                if progress is not None:
                    progress(
                        ExtractionProgress(
                            completed_before + succeeded + failed,
                            total,
                            batch_index,
                            len(batches),
                            f"{phase}: failed",
                            succeeded,
                            failed,
                        )
                    )
                break
            else:
                succeeded += processed
                accepted_cases += case_count
                exporter()
                detail = canonical_json(
                    {
                        "event": "llm_batch_completed",
                        "phase": phase,
                        "batch": batch_index,
                        "batch_id": batch_id,
                        "articles": len(story_ids),
                        "cases": case_count,
                        "attempts": attempt_number,
                        "request_id": response.request_id,
                        "structured_output": response.structured_output,
                    }
                )
                details.append(detail)
                if detail_reporter is not None:
                    detail_reporter(detail)
                if progress is not None:
                    progress(
                        ExtractionProgress(
                            completed_before + succeeded + failed,
                            total,
                            batch_index,
                            len(batches),
                            f"{phase}: completed",
                            succeeded,
                            failed,
                        )
                    )
                break
    return succeeded, failed, accepted_cases, details


def extract_topic(
    config: AppConfig,
    topic_name: str,
    connection: sqlite3.Connection,
    *,
    limit: int | None = None,
    dry_run: bool = False,
    credentials: LLMCredentials | None = None,
    output_dir: Path = Path("data/out"),
    _client: ExtractionClient | None = None,
    _rate_limiter: TokenBucket | None = None,
    _exporter: ExportCallback | None = None,
    progress: ExtractionProgressReporter | None = None,
    detail_reporter: ExtractionDetailReporter | None = None,
) -> StageSummary:
    """Run exact screening, then detailed extraction for screening-positive articles."""
    if topic_name not in config.topics:
        raise ConfigError(f"Unknown topic: {topic_name}")
    topic = config.topics[topic_name]
    estimator = TokenEstimator(config.llm.model, allow_tokenizer_load=not dry_run)
    screening_candidates = _candidates_for_phase(
        connection,
        topic_name,
        config.llm.screening_prompt_version,
        topic.fetch_priority_title_terms,
        limit,
        "screening",
        screening_prompt_version=config.llm.screening_prompt_version,
    )
    screening_batches = pack_candidates(
        screening_candidates,
        topic=topic,
        estimator=estimator,
        max_input_tokens=config.llm.screening_input_tokens,
        phase="screening",
        max_articles=None,
    )
    extraction_candidates = _candidates_for_phase(
        connection,
        topic_name,
        config.llm.prompt_version,
        topic.fetch_priority_title_terms,
        limit,
        "extraction",
        screening_prompt_version=config.llm.screening_prompt_version,
    )
    extraction_batches = pack_candidates(
        extraction_candidates,
        topic=topic,
        estimator=estimator,
        max_input_tokens=config.llm.extraction_input_tokens,
        phase="extraction",
        max_articles=config.llm.extraction_max_articles_per_batch,
    )
    details = list(_planned_details(screening_batches)) + list(_planned_details(extraction_batches))
    if dry_run:
        total = len(screening_candidates) + len(extraction_candidates)
        return StageSummary(topic_name, total, 0, total, 0, tuple(details))
    if credentials is None:
        raise ExtractionError("LLM credentials are required for extraction.")
    limiter = _rate_limiter or TokenBucket(config.llm.max_requests_per_min, capacity=1)
    client = _client or OpenAIExtractionClient(config.llm, credentials)
    if _exporter is None:
        from .export import export_topic

        def exporter() -> StageSummary:
            return export_topic(config, topic_name, connection, output_dir=output_dir)
    else:
        exporter = _exporter
    config_hash, config_json, revision, versions_json = _provenance(config, topic_name)
    run_id = start_pipeline_run(
        connection,
        run_uuid=str(uuid.uuid4()),
        topic=topic_name,
        stage="extract",
        config_hash=config_hash,
        config_json=config_json,
        git_revision=revision,
        package_versions_json=versions_json,
    )
    try:
        screen_ok, screen_failed, _, screen_details = _run_phase(
            connection,
            phase="screening",
            batches=screening_batches,
            topic_name=topic_name,
            config=config,
            credentials=credentials,
            client=client,
            limiter=limiter,
            run_id=run_id,
            progress=progress,
            detail_reporter=detail_reporter,
            total=len(screening_candidates),
            completed_before=0,
            exporter=exporter,
        )
        details.extend(screen_details)
        # Re-select after screening so this invocation immediately extracts newly positive articles.
        extraction_candidates = _candidates_for_phase(
            connection,
            topic_name,
            config.llm.prompt_version,
            topic.fetch_priority_title_terms,
            limit,
            "extraction",
            screening_prompt_version=config.llm.screening_prompt_version,
        )
        extraction_batches = pack_candidates(
            extraction_candidates,
            topic=topic,
            estimator=estimator,
            max_input_tokens=config.llm.extraction_input_tokens,
            phase="extraction",
            max_articles=config.llm.extraction_max_articles_per_batch,
        )
        extract_ok, extract_failed, accepted_cases, extract_details = _run_phase(
            connection,
            phase="extraction",
            batches=extraction_batches,
            topic_name=topic_name,
            config=config,
            credentials=credentials,
            client=client,
            limiter=limiter,
            run_id=run_id,
            progress=progress,
            detail_reporter=detail_reporter,
            total=screen_ok + screen_failed + len(extraction_candidates),
            completed_before=screen_ok + screen_failed,
            exporter=exporter,
        )
        details.extend(extract_details)
        if not screening_batches and not extraction_batches:
            exporter()
    except BaseException as exc:
        complete_pipeline_run(connection, run_id, status="failed", error=str(exc)[:1000])
        raise
    complete_pipeline_run(connection, run_id, status="completed")
    details.append(
        canonical_json(
            {
                "event": "llm_run_completed",
                "screened_articles": screen_ok,
                "screening_failed_articles": screen_failed,
                "extracted_articles": extract_ok,
                "extraction_failed_articles": extract_failed,
                "accepted_cases": accepted_cases,
            }
        )
    )
    return StageSummary(
        topic_name,
        screen_ok + extract_ok,
        screen_ok + extract_ok,
        0,
        screen_failed + extract_failed,
        tuple(details),
    )
