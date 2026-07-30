"""Stage 4: packed, serial structured extraction over stored article text."""

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
from typing import Any, Protocol, cast

import tiktoken
from pydantic import ValidationError

from .config import AppConfig, LLMCredentials
from .contracts import build_response_json_schema, build_response_model
from .db import complete_pipeline_run, start_pipeline_run, transaction
from .errors import ConfigError, ExtractionError
from .identity import build_case_id, canonical_json, sha256_hex
from .llm import (
    LLMResponse,
    OpenAIExtractionClient,
    parse_json_content,
)
from .prompt import SYSTEM_PROMPT, USER_TEMPLATE_VERSION, PromptArticle, build_extraction_prompt
from .ratelimit import TokenBucket
from .stage import StageSummary, git_revision, package_version

ExportCallback = Callable[[], StageSummary]


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


@dataclass(frozen=True)
class PackedBatch:
    articles: tuple[PromptArticle, ...]
    user_prompt: str
    estimated_tokens: int


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


def build_manual_request(
    config: AppConfig,
    topic_name: str,
    connection: sqlite3.Connection,
    *,
    limit: int | None = None,
    batch_number: int = 1,
    extraction_batch_id: int | None = None,
) -> dict[str, Any]:
    """Build one exact secret-free pair of prompts for manual LLM verification."""
    if topic_name not in config.topics:
        raise ConfigError(f"Unknown topic: {topic_name}")
    if batch_number < 1:
        raise ConfigError("batch_number must be at least 1")
    topic = config.topics[topic_name]
    if extraction_batch_id is not None:
        if limit is not None or batch_number != 1:
            raise ConfigError("--batch-id cannot be combined with --limit or --batch")
        stored = connection.execute(
            """
            SELECT id, topic, model, prompt_version, story_ids_json, request_json
            FROM extraction_batches
            WHERE id = ?
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
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ConfigError(
                f"Extraction batch {extraction_batch_id} has invalid stored request provenance"
            ) from exc
        return {
            "topic": topic_name,
            "prompt_version": str(stored["prompt_version"]),
            "source_extraction_batch_id": extraction_batch_id,
            "story_ids": story_ids,
            "estimated_input_tokens": estimated_tokens,
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
        }
    if limit is None or limit < 1:
        raise ConfigError("--limit is required when --batch-id is not provided")
    candidates = _candidate_rows(
        connection,
        topic_name,
        config.llm.prompt_version,
        topic.fetch_priority_title_terms,
        limit,
    )
    batches = pack_candidates(
        candidates,
        extraction=topic.extraction,
        estimator=TokenEstimator(config.llm.model, allow_tokenizer_load=False),
        max_input_tokens=config.llm.max_input_tokens,
    )
    if not batches:
        raise ConfigError(f"No pending extraction candidates for topic: {topic_name}")
    if batch_number > len(batches):
        raise ConfigError(
            f"Batch {batch_number} does not exist; dry run produced {len(batches)} batches"
        )
    batch = batches[batch_number - 1]
    return {
        "topic": topic_name,
        "prompt_version": config.llm.prompt_version,
        "batch_number": batch_number,
        "batch_count": len(batches),
        "story_ids": [article.story_id for article in batch.articles],
        "estimated_input_tokens": batch.estimated_tokens,
        "system_prompt": SYSTEM_PROMPT,
        "user_prompt": batch.user_prompt,
    }


def write_manual_request(artifact: dict[str, Any], output: Path) -> Path:
    """Atomically write one human-readable system and user prompt pair."""
    output.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=output.parent,
        prefix=f".{output.name}.",
        suffix=".tmp",
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
            tokens = self._encoding.encode(text)
            return str(self._encoding.decode(tokens[:max_tokens]))
        encoded = text.encode("utf-8")[: max_tokens * 2]
        return encoded.decode("utf-8", errors="ignore")


def _candidate_rows(
    connection: sqlite3.Connection,
    topic_name: str,
    prompt_version: str,
    priority_terms: Sequence[str],
    limit: int | None,
) -> list[Candidate]:
    priority_sql = "0"
    priority_parameters: list[Any] = []
    if priority_terms:
        clauses = []
        for term in priority_terms:
            clauses.append("LOWER(COALESCE(s.title, '')) LIKE ?")
            priority_parameters.append(f"%{term.casefold()}%")
        priority_sql = f"CASE WHEN {' OR '.join(clauses)} THEN 0 ELSE 1 END"
    limit_sql = ""
    parameters: list[Any] = [prompt_version, topic_name, *priority_parameters]
    if limit is not None:
        limit_sql = "LIMIT ?"
        parameters.append(limit)
    rows = connection.execute(
        f"""
        SELECT s.story_id, COALESCE(s.title, '') AS title, a.text
        FROM story_topic_state AS sts
        JOIN stories AS s ON s.story_id = sts.story_id
        JOIN articles AS a ON a.story_id = s.story_id
        LEFT JOIN story_extractions AS se
          ON se.topic = sts.topic
         AND se.story_id = sts.story_id
         AND se.prompt_version = ?
        WHERE sts.topic = ?
          AND sts.qc_status = 'ok'
          AND a.fetch_status = 'ok'
          AND a.text IS NOT NULL
          AND LENGTH(TRIM(a.text)) > 0
          AND (se.id IS NULL OR se.validation_status IN ('invalid', 'unparsed'))
        ORDER BY CASE WHEN se.id IS NULL THEN 0 ELSE 1 END,
                 {priority_sql}, se.extracted_at, s.publish_date, s.story_id
        {limit_sql}
        """,
        parameters,
    ).fetchall()
    return [Candidate(str(row["story_id"]), str(row["title"]), str(row["text"])) for row in rows]


def _estimate_prompt(estimator: TokenEstimator, user_prompt: str) -> int:
    return estimator.count(SYSTEM_PROMPT) + estimator.count(user_prompt) + 16


def pack_candidates(
    candidates: Sequence[Candidate],
    *,
    extraction: Any,
    estimator: TokenEstimator,
    max_input_tokens: int,
) -> list[PackedBatch]:
    """Greedily pack candidates below the configured complete-input token ceiling."""
    batches: list[PackedBatch] = []
    current: list[PromptArticle] = []
    for candidate in candidates:
        article = PromptArticle(candidate.story_id, candidate.title, candidate.text)
        proposed = (*current, article)
        proposed_prompt = build_extraction_prompt(extraction, proposed)
        proposed_tokens = _estimate_prompt(estimator, proposed_prompt)
        if current and proposed_tokens > max_input_tokens:
            current_prompt = build_extraction_prompt(extraction, current)
            batches.append(
                PackedBatch(
                    tuple(current),
                    current_prompt,
                    _estimate_prompt(estimator, current_prompt),
                )
            )
            current = []
            proposed = (article,)
            proposed_prompt = build_extraction_prompt(extraction, proposed)
            proposed_tokens = _estimate_prompt(estimator, proposed_prompt)
        if proposed_tokens > max_input_tokens:
            empty_prompt = build_extraction_prompt(
                extraction, (PromptArticle(candidate.story_id, candidate.title, ""),)
            )
            available = max_input_tokens - _estimate_prompt(estimator, empty_prompt)
            if available <= 0:
                raise ConfigError(
                    "max_input_tokens is too small for the extraction schema and article labels"
                )
            article = PromptArticle(
                candidate.story_id,
                candidate.title,
                estimator.truncate(candidate.text, available),
            )
        current.append(article)
    if current:
        prompt = build_extraction_prompt(extraction, current)
        batches.append(PackedBatch(tuple(current), prompt, _estimate_prompt(estimator, prompt)))
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
            "system_prompt": SYSTEM_PROMPT,
            "user_prompt": batch.user_prompt,
            "estimated_input_tokens": batch.estimated_tokens,
        }
    )
    with transaction(connection):
        cursor = connection.execute(
            """
            INSERT INTO extraction_batches(
                pipeline_run_id, topic, model, base_url, prompt_version, prompt_hash,
                schema_hash, status, story_ids_json, attempt_count, request_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, 0, ?)
            """,
            (
                run_id,
                topic_name,
                config.llm.model,
                credentials.base_url,
                config.llm.prompt_version,
                prompt_hash,
                schema_hash,
                canonical_json([article.story_id for article in batch.articles]),
                request_json,
            ),
        )
        if cursor.lastrowid is None:
            raise ExtractionError("Failed to create extraction batch.")
        return cursor.lastrowid


def _story_failure_rows(
    connection: sqlite3.Connection,
    *,
    batch_id: int,
    topic_name: str,
    story_ids: Sequence[str],
    config: AppConfig,
    validation_status: str,
    error: str,
    attempt_count: int,
    response: LLMResponse | None = None,
) -> None:
    now = datetime.now(UTC).isoformat()
    with transaction(connection):
        connection.executemany(
            """
            INSERT INTO story_extractions(
                topic, story_id, extraction_batch_id, model, prompt_version, relevant,
                reject_reason, validation_status, validation_error, usage_json, raw_json,
                extracted_at
            )
            VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?)
            ON CONFLICT(topic, story_id, prompt_version) DO UPDATE SET
                extraction_batch_id = excluded.extraction_batch_id,
                model = excluded.model,
                relevant = NULL,
                reject_reason = NULL,
                validation_status = excluded.validation_status,
                validation_error = excluded.validation_error,
                payload_json = NULL,
                usage_json = excluded.usage_json,
                raw_json = excluded.raw_json,
                extracted_at = excluded.extracted_at
            """,
            [
                (
                    topic_name,
                    story_id,
                    batch_id,
                    config.llm.model,
                    config.llm.prompt_version,
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
            UPDATE extraction_batches
            SET status = ?, attempt_count = ?, response_json = ?, usage_json = ?, raw_json = ?,
                completed_at = CURRENT_TIMESTAMP, error = ?
            WHERE id = ?
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


def _persist_valid_response(
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
    response_model = build_response_model(config.topics[topic_name].extraction)
    validated = response_model.model_validate(payload)
    results = cast(Any, validated).results
    returned_ids = [str(result.story_id) for result in results]
    if len(returned_ids) != len(set(returned_ids)) or not set(returned_ids) <= set(expected_ids):
        raise ValueError("response contained duplicate or unknown story_id values")

    by_id = {str(result.story_id): result for result in results}
    placeholders = ",".join("?" for _ in expected_ids)
    story_rows = connection.execute(
        f"SELECT story_id, media_name, url FROM stories WHERE story_id IN ({placeholders})",
        tuple(expected_ids),
    ).fetchall()
    provenance = {str(row["story_id"]): row for row in story_rows}
    now = datetime.now(UTC).isoformat()
    case_count = 0
    with transaction(connection):
        for story_id in expected_ids:
            result = by_id.get(story_id)
            relevant = result is not None
            payload_json = canonical_json(result.model_dump(mode="json")) if result else None
            connection.execute(
                """
                INSERT INTO story_extractions(
                    topic, story_id, extraction_batch_id, model, prompt_version, relevant,
                    reject_reason, validation_status, payload_json, usage_json, raw_json,
                    extracted_at
                )
                VALUES (?, ?, ?, ?, ?, ?, NULL, 'valid', ?, ?, ?, ?)
                ON CONFLICT(topic, story_id, prompt_version) DO UPDATE SET
                    extraction_batch_id = excluded.extraction_batch_id,
                    model = excluded.model,
                    relevant = excluded.relevant,
                    reject_reason = NULL,
                    validation_status = 'valid',
                    validation_error = NULL,
                    payload_json = excluded.payload_json,
                    usage_json = excluded.usage_json,
                    raw_json = excluded.raw_json,
                    extracted_at = excluded.extracted_at
                """,
                (
                    topic_name,
                    story_id,
                    batch_id,
                    config.llm.model,
                    config.llm.prompt_version,
                    int(relevant),
                    payload_json,
                    canonical_json(response.usage) if response.usage else None,
                    _response_raw_json(response),
                    now,
                ),
            )
            extraction_row = connection.execute(
                """
                SELECT id FROM story_extractions
                WHERE topic = ? AND story_id = ? AND prompt_version = ?
                """,
                (topic_name, story_id, config.llm.prompt_version),
            ).fetchone()
            if extraction_row is None:
                raise ExtractionError(f"Failed to persist extraction for {story_id}.")
            extraction_id = int(extraction_row["id"])
            connection.execute("DELETE FROM cases WHERE story_extraction_id = ?", (extraction_id,))
            if result is None:
                continue
            for ordinal, case in enumerate(result.cases):
                values = case.model_dump(mode="json")
                story = provenance[story_id]
                person_name = values.get("person_name")
                cohort_name = values.get("cohort_name")
                connection.execute(
                    """
                    INSERT INTO cases(
                        case_id, story_extraction_id, case_type, person_name, cohort_name,
                        link_category, previous_org, previous_sector, current_org, current_sector,
                        jurisdiction, expected_resolvable, status, source_outlet, source_url, claim,
                        evidence_verified, raw_json, private_org, private_time, public_org,
                        public_time
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'private', ?, 'public', ?, 'yes', 'accepted',
                            ?, ?, ?, 0, ?, ?, ?, ?, ?)
                    """,
                    (
                        build_case_id(
                            topic=topic_name,
                            story_id=story_id,
                            prompt_version=config.llm.prompt_version,
                            ordinal=ordinal,
                        ),
                        extraction_id,
                        "cohort" if cohort_name else "individual",
                        person_name,
                        cohort_name,
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
            UPDATE extraction_batches
            SET status = 'completed', attempt_count = ?, response_json = ?, usage_json = ?,
                raw_json = ?, completed_at = CURRENT_TIMESTAMP, error = NULL
            WHERE id = ?
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


def _response_raw_json(response: LLMResponse) -> str:
    return canonical_json(
        {
            "provider_response": response.raw,
            "request_id": response.request_id,
            "structured_output": response.structured_output,
        }
    )


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
) -> StageSummary:
    """Extract config-shaped cases and refresh CSV snapshots after each batch."""
    if topic_name not in config.topics:
        raise ConfigError(f"Unknown topic: {topic_name}")
    candidates = _candidate_rows(
        connection,
        topic_name,
        config.llm.prompt_version,
        config.topics[topic_name].fetch_priority_title_terms,
        limit,
    )
    estimator = TokenEstimator(config.llm.model, allow_tokenizer_load=not dry_run)
    batches = pack_candidates(
        candidates,
        extraction=config.topics[topic_name].extraction,
        estimator=estimator,
        max_input_tokens=config.llm.max_input_tokens,
    )
    planned_details = tuple(
        canonical_json(
            {
                "endpoint": "llm_extract",
                "batch": index,
                "articles": len(batch.articles),
                "estimated_input_tokens": batch.estimated_tokens,
                "story_ids": [article.story_id for article in batch.articles],
            }
        )
        for index, batch in enumerate(batches, start=1)
    )
    if dry_run:
        return StageSummary(topic_name, len(candidates), 0, len(candidates), 0, planned_details)
    if credentials is None:
        raise ExtractionError("LLM credentials are required for extraction.")

    schema = build_response_json_schema(config.topics[topic_name].extraction)
    prompt_hash = sha256_hex(
        {"system": SYSTEM_PROMPT, "user_template": USER_TEMPLATE_VERSION, "schema": schema}
    )
    schema_hash = sha256_hex(schema)
    limiter = _rate_limiter or TokenBucket(config.llm.max_requests_per_min, capacity=1)
    client = _client or OpenAIExtractionClient(
        config.llm, credentials, before_request=limiter.acquire
    )
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
    succeeded = 0
    failed = 0
    accepted_cases = 0
    run_details = list(planned_details)
    try:
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
            max_attempts = config.llm.max_retries + 1
            if progress is not None:
                progress(
                    ExtractionProgress(
                        succeeded + failed,
                        len(candidates),
                        batch_index,
                        len(batches),
                        "requesting",
                        succeeded,
                        failed,
                    )
                )
            for attempt_number in range(1, max_attempts + 1):
                if _client is not None:
                    limiter.acquire()
                response: LLMResponse | None = None
                try:
                    response = client.extract(
                        system_prompt=SYSTEM_PROMPT,
                        user_prompt=batch.user_prompt,
                        schema=schema,
                    )
                    payload = parse_json_content(response.content)
                    processed, case_count = _persist_valid_response(
                        connection,
                        batch_id=batch_id,
                        topic_name=topic_name,
                        expected_ids=story_ids,
                        config=config,
                        payload=payload,
                        response=response,
                        attempt_count=attempt_number,
                    )
                except (ExtractionError, ValidationError, ValueError) as exc:
                    run_details.append(
                        canonical_json(
                            {
                                "event": "llm_attempt_failed",
                                "batch": batch_index,
                                "batch_id": batch_id,
                                "attempt": attempt_number,
                                "max_attempts": max_attempts,
                                "error": str(exc)[:1000],
                            }
                        )
                    )
                    if attempt_number < max_attempts:
                        if progress is not None:
                            progress(
                                ExtractionProgress(
                                    succeeded + failed,
                                    len(candidates),
                                    batch_index,
                                    len(batches),
                                    f"retrying {attempt_number + 1}/{max_attempts}",
                                    succeeded,
                                    failed,
                                )
                            )
                        continue
                    _story_failure_rows(
                        connection,
                        batch_id=batch_id,
                        topic_name=topic_name,
                        story_ids=story_ids,
                        config=config,
                        validation_status=(
                            "invalid"
                            if isinstance(exc, (ValidationError, ValueError))
                            else "unparsed"
                        ),
                        error=str(exc),
                        attempt_count=attempt_number,
                        response=response,
                    )
                    failed += len(story_ids)
                    run_details.append(
                        canonical_json(
                            {
                                "event": "llm_batch_failed",
                                "batch": batch_index,
                                "batch_id": batch_id,
                                "articles": len(story_ids),
                                "attempts": attempt_number,
                                "queue": "retryable_tail",
                                "error": str(exc)[:1000],
                            }
                        )
                    )
                    if progress is not None:
                        progress(
                            ExtractionProgress(
                                succeeded + failed,
                                len(candidates),
                                batch_index,
                                len(batches),
                                "failed",
                                succeeded,
                                failed,
                            )
                        )
                else:
                    succeeded += processed
                    accepted_cases += case_count
                    run_details.append(
                        canonical_json(
                            {
                                "event": "llm_batch_completed",
                                "batch": batch_index,
                                "batch_id": batch_id,
                                "articles": len(story_ids),
                                "cases": case_count,
                                "attempts": attempt_number,
                                "structured_output": response.structured_output,
                                "request_id": response.request_id,
                            }
                        )
                    )
                    if progress is not None:
                        progress(
                            ExtractionProgress(
                                succeeded + failed,
                                len(candidates),
                                batch_index,
                                len(batches),
                                "completed",
                                succeeded,
                                failed,
                            )
                        )
                    break
            exporter()
    except BaseException as exc:
        complete_pipeline_run(connection, run_id, status="failed", error=str(exc)[:1000])
        raise
    complete_pipeline_run(connection, run_id, status="completed")
    run_details.append(
        canonical_json(
            {
                "event": "llm_run_completed",
                "processed_articles": len(candidates),
                "successful_articles": succeeded,
                "failed_articles": failed,
                "accepted_cases": accepted_cases,
            }
        )
    )
    return StageSummary(topic_name, len(candidates), succeeded, 0, failed, tuple(run_details))
