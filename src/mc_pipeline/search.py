"""Media Cloud estimation, paginated search, and metadata review export."""

from __future__ import annotations

import csv
import json
import platform as python_platform
import sqlite3
import subprocess
import time
import uuid
from collections.abc import Callable, Mapping
from datetime import date, timedelta
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Protocol, cast
from urllib.parse import urlsplit

from mediacloud.api import SearchApi
from mediacloud.error import APIResponseError
from pydantic import SecretStr
from requests import RequestException

from .config import AppConfig, MediaCloudConfig, TopicConfig
from .db import (
    complete_pipeline_run,
    get_search_window,
    mark_search_window_complete,
    mark_search_window_failed,
    persist_search_page,
    search_review_rows,
    search_review_stats,
    start_pipeline_run,
    update_search_window_estimate,
    upsert_search_window,
)
from .errors import ConfigError, MediaCloudError, MissingCredentialError
from .identity import canonical_json, semantic_query_hash, sha256_hex, window_fingerprint
from .stage import SearchReviewSummary, StageSummary


class SearchClient(Protocol):
    TIMEOUT_SECS: float

    def story_count(
        self,
        query: str,
        start_date: date,
        end_date: date,
        collection_ids: list[int],
        source_ids: list[int],
        platform: str,
    ) -> Mapping[str, int]: ...

    def story_list(
        self,
        query: str,
        start_date: date,
        end_date: date,
        collection_ids: list[int],
        source_ids: list[int],
        platform: str,
        expanded: bool,
        pagination_token: str | None,
        page_size: int,
    ) -> tuple[list[Mapping[str, Any]], str | None]: ...


ClientFactory = Callable[[str], SearchClient]


def split_date_windows(
    start_date: date, end_date: date, window_days: int
) -> list[tuple[date, date]]:
    """Split an inclusive date range into closed, non-overlapping windows."""
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date")
    if window_days <= 0:
        raise ValueError("window_days must be positive")

    windows: list[tuple[date, date]] = []
    window_start = start_date
    while window_start <= end_date:
        window_end = min(end_date, window_start + timedelta(days=window_days - 1))
        windows.append((window_start, window_end))
        window_start = window_end + timedelta(days=1)
    return windows


def _topic(config: AppConfig, topic_name: str) -> TopicConfig:
    try:
        topic = config.topics[topic_name]
    except KeyError as exc:
        raise ConfigError(f"Unknown topic: {topic_name}") from exc
    if not topic.collection_ids and not topic.source_ids:
        raise ConfigError(f"Topic {topic_name!r} requires a collection_id or source_id")
    return topic


def _package_version(package: str) -> str:
    try:
        return version(package)
    except PackageNotFoundError:
        return "unknown"


def _git_revision() -> str:
    try:
        revision = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return "uncommitted"

    dirty = subprocess.run(
        ["git", "status", "--short"],
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    ).stdout.strip()
    return f"{revision}-dirty" if dirty else revision


def _provenance(config: AppConfig, topic_name: str) -> tuple[str, str, str, str]:
    relevant_config = {
        "media_cloud": config.media_cloud.model_dump(mode="json"),
        "topic": config.topics[topic_name].model_dump(mode="json"),
    }
    config_json = canonical_json(relevant_config)
    versions_json = canonical_json(
        {
            "python": python_platform.python_version(),
            "media-cloud-pipeline": _package_version("media-cloud-pipeline"),
            "mediacloud": _package_version("mediacloud"),
        }
    )
    return sha256_hex(relevant_config), config_json, _git_revision(), versions_json


def _default_client_factory(token: str) -> SearchClient:
    return cast(SearchClient, SearchApi(token))


def _new_client(
    api_token: SecretStr, config: MediaCloudConfig, factory: ClientFactory
) -> SearchClient:
    client = factory(api_token.get_secret_value())
    client.TIMEOUT_SECS = config.timeout_s
    return client


def _call_once(operation: Callable[[], Any], *, operation_name: str) -> Any:
    """Call Media Cloud once so failed windows can be persisted immediately."""
    try:
        return operation()
    except (APIResponseError, RequestException) as exc:
        status = exc.response.status_code if isinstance(exc, APIResponseError) else None
        detail = f" HTTP {status}" if status is not None else " transport error"
        raise MediaCloudError(f"Media Cloud {operation_name} failed:{detail}.") from exc


def _window_inputs(
    config: AppConfig, topic: TopicConfig, window_start: date, window_end: date
) -> dict[str, Any]:
    return {
        "query": topic.query,
        "platform": config.media_cloud.platform,
        "collection_ids": topic.collection_ids,
        "source_ids": topic.source_ids,
        "languages": topic.languages,
        "window_start": window_start,
        "window_end": window_end,
    }


def _window_metadata(raw_json: str | None) -> dict[str, Any]:
    if not raw_json:
        return {"estimate": None, "pages": []}
    try:
        parsed = json.loads(raw_json)
    except json.JSONDecodeError:
        return {"estimate": None, "pages": []}
    if not isinstance(parsed, dict):
        return {"estimate": None, "pages": []}
    parsed.setdefault("estimate", None)
    parsed.setdefault("pages", [])
    return parsed


def _normalized_story(story: Mapping[str, Any], result_rank: int) -> dict[str, Any]:
    story_id = story.get("id")
    if not isinstance(story_id, str) or not story_id:
        raise MediaCloudError("Media Cloud returned a story without a usable string id.")

    url = story.get("url")
    url_text = str(url) if url is not None else None
    hostname = urlsplit(url_text).hostname if url_text else None
    publish_date = story.get("publish_date")
    indexed_date = story.get("indexed_date")
    text = story.get("text")
    return {
        "story_id": story_id,
        "title": str(story["title"]) if story.get("title") is not None else None,
        "url": url_text,
        "domain": hostname.lower() if hostname else None,
        "publish_date": publish_date.isoformat() if isinstance(publish_date, date) else None,
        "media_name": str(story["media_name"]) if story.get("media_name") is not None else None,
        "media_url": str(story["media_url"]) if story.get("media_url") is not None else None,
        "language": str(story["language"]) if story.get("language") is not None else None,
        "indexed_at": indexed_date.isoformat() if isinstance(indexed_date, date) else None,
        "mc_text": text if isinstance(text, str) else None,
        "raw_json": canonical_json(dict(story)),
        "result_rank": result_rank,
    }


def _window_id(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    topic_name: str,
    topic: TopicConfig,
    config: AppConfig,
    window_start: date,
    window_end: date,
    query_hash: str,
    fingerprint: str,
) -> int:
    return upsert_search_window(
        connection,
        pipeline_run_id=run_id,
        topic=topic_name,
        query_hash=query_hash,
        request_fingerprint=fingerprint,
        query=topic.query,
        platform=config.media_cloud.platform,
        collection_ids_json=canonical_json(topic.collection_ids),
        source_ids_json=canonical_json(topic.source_ids),
        languages_json=canonical_json(topic.languages),
        window_start=window_start.isoformat(),
        window_end=window_end.isoformat(),
        mediacloud_version=_package_version("mediacloud"),
    )


def estimate_topic(
    config: AppConfig,
    topic_name: str,
    connection: sqlite3.Connection,
    *,
    dry_run: bool = False,
    api_token: SecretStr | None = None,
    _client_factory: ClientFactory = _default_client_factory,
) -> StageSummary:
    """Estimate each topic window with ``story_count`` only."""
    topic = _topic(config, topic_name)
    windows = split_date_windows(topic.start_date, topic.end_date, config.media_cloud.window_days)
    if dry_run:
        return StageSummary(topic_name, len(windows), 0, len(windows), 0)
    if api_token is None:
        raise MissingCredentialError("Media Cloud credentials are required for estimate")

    config_hash, config_json, git_revision, versions_json = _provenance(config, topic_name)
    run_id = start_pipeline_run(
        connection,
        run_uuid=str(uuid.uuid4()),
        topic=topic_name,
        stage="estimate",
        config_hash=config_hash,
        config_json=config_json,
        git_revision=git_revision,
        package_versions_json=versions_json,
    )
    client = _new_client(api_token, config.media_cloud, _client_factory)
    query_hash = semantic_query_hash(
        query=topic.query,
        platform=config.media_cloud.platform,
        collection_ids=topic.collection_ids,
        source_ids=topic.source_ids,
        languages=topic.languages,
    )
    succeeded = skipped = failed = 0
    try:
        for window_start, window_end in windows:
            inputs = _window_inputs(config, topic, window_start, window_end)
            fingerprint = window_fingerprint(**inputs)
            existing = get_search_window(
                connection,
                topic=topic_name,
                request_fingerprint=fingerprint,
                window_start=window_start.isoformat(),
                window_end=window_end.isoformat(),
            )
            if existing is not None and (
                existing["pagination_completed"]
                or existing["relevant_count"] is not None
                or existing["status"] in {"failed", "page_limit"}
            ):
                skipped += 1
                continue
            window_id = _window_id(
                connection,
                run_id=run_id,
                topic_name=topic_name,
                topic=topic,
                config=config,
                window_start=window_start,
                window_end=window_end,
                query_hash=query_hash,
                fingerprint=fingerprint,
            )
            started = time.monotonic()

            def count_operation(
                start: date = window_start, end: date = window_end
            ) -> Mapping[str, int]:
                return client.story_count(
                    topic.query,
                    start,
                    end,
                    collection_ids=list(topic.collection_ids),
                    source_ids=list(topic.source_ids),
                    platform=config.media_cloud.platform,
                )

            metadata = _window_metadata(existing["raw_json"] if existing is not None else None)
            try:
                count = cast(
                    Mapping[str, int], _call_once(count_operation, operation_name="story_count")
                )
            except MediaCloudError as exc:
                mark_search_window_failed(
                    connection,
                    window_id,
                    status="failed",
                    error=str(exc),
                    raw_json=canonical_json(metadata),
                )
                failed += 1
                continue
            metadata["estimate"] = {
                "endpoint": "story_count",
                "elapsed_ms": round((time.monotonic() - started) * 1000),
                "result": dict(count),
            }
            update_search_window_estimate(
                connection,
                window_id,
                relevant_count=count.get("relevant"),
                total_count=count.get("total"),
                raw_json=canonical_json(metadata),
            )
            succeeded += 1
    except MediaCloudError as exc:
        complete_pipeline_run(connection, run_id, status="failed", error=str(exc))
        raise
    complete_pipeline_run(connection, run_id, status="completed")
    return StageSummary(topic_name, len(windows), succeeded, skipped, failed)


def search_topic(
    config: AppConfig,
    topic_name: str,
    connection: sqlite3.Connection,
    *,
    limit_windows: int | None = None,
    dry_run: bool = False,
    api_token: SecretStr | None = None,
    _client_factory: ClientFactory = _default_client_factory,
) -> StageSummary:
    """Search and persist Media Cloud pages for a configured topic."""
    topic = _topic(config, topic_name)
    windows = split_date_windows(topic.start_date, topic.end_date, config.media_cloud.window_days)
    if limit_windows is not None:
        if limit_windows <= 0:
            raise ConfigError("limit_windows must be positive")
        windows = windows[:limit_windows]
    if dry_run:
        cached = 0
        for window_start, window_end in windows:
            fingerprint = window_fingerprint(
                **_window_inputs(config, topic, window_start, window_end)
            )
            existing = get_search_window(
                connection,
                topic=topic_name,
                request_fingerprint=fingerprint,
                window_start=window_start.isoformat(),
                window_end=window_end.isoformat(),
            )
            cached += int(existing is not None and bool(existing["pagination_completed"]))
        return StageSummary(topic_name, len(windows), 0, cached, 0)
    if api_token is None:
        raise MissingCredentialError("Media Cloud credentials are required for search")

    config_hash, config_json, git_revision, versions_json = _provenance(config, topic_name)
    run_id = start_pipeline_run(
        connection,
        run_uuid=str(uuid.uuid4()),
        topic=topic_name,
        stage="search",
        config_hash=config_hash,
        config_json=config_json,
        git_revision=git_revision,
        package_versions_json=versions_json,
    )
    client = _new_client(api_token, config.media_cloud, _client_factory)
    query_hash = semantic_query_hash(
        query=topic.query,
        platform=config.media_cloud.platform,
        collection_ids=topic.collection_ids,
        source_ids=topic.source_ids,
        languages=topic.languages,
    )
    succeeded = skipped = failed = 0
    try:
        for window_start, window_end in windows:
            inputs = _window_inputs(config, topic, window_start, window_end)
            fingerprint = window_fingerprint(**inputs)
            existing = get_search_window(
                connection,
                topic=topic_name,
                request_fingerprint=fingerprint,
                window_start=window_start.isoformat(),
                window_end=window_end.isoformat(),
            )
            if existing is not None and (
                existing["pagination_completed"] or existing["status"] in {"failed", "page_limit"}
            ):
                skipped += 1
                continue
            window_id = _window_id(
                connection,
                run_id=run_id,
                topic_name=topic_name,
                topic=topic,
                config=config,
                window_start=window_start,
                window_end=window_end,
                query_hash=query_hash,
                fingerprint=fingerprint,
            )
            current = get_search_window(
                connection,
                topic=topic_name,
                request_fingerprint=fingerprint,
                window_start=window_start.isoformat(),
                window_end=window_end.isoformat(),
            )
            if current is None:
                raise MediaCloudError("Search window disappeared after creation.")
            page_number = int(current["pages_fetched"]) + 1
            token = cast(str | None, current["next_pagination_token"])
            metadata = _window_metadata(cast(str | None, current["raw_json"]))
            try:
                while True:
                    if page_number > config.media_cloud.max_pages_per_window:
                        message = (
                            f"Window {window_start.isoformat()}..{window_end.isoformat()} reached "
                            f"max_pages_per_window={config.media_cloud.max_pages_per_window}."
                        )
                        mark_search_window_failed(
                            connection,
                            window_id,
                            status="page_limit",
                            error=message,
                            raw_json=canonical_json(metadata),
                        )
                        failed += 1
                        break

                    started = time.monotonic()

                    def list_operation(
                        start: date = window_start,
                        end: date = window_end,
                        current_token: str | None = token,
                    ) -> tuple[list[Mapping[str, Any]], str | None]:
                        return client.story_list(
                            topic.query,
                            start,
                            end,
                            collection_ids=list(topic.collection_ids),
                            source_ids=list(topic.source_ids),
                            platform=config.media_cloud.platform,
                            expanded=False,
                            pagination_token=current_token,
                            page_size=config.media_cloud.page_size,
                        )

                    stories, next_token = cast(
                        tuple[list[Mapping[str, Any]], str | None],
                        _call_once(list_operation, operation_name="story_list"),
                    )
                    normalized = [
                        _normalized_story(
                            story,
                            (page_number - 1) * config.media_cloud.page_size + offset,
                        )
                        for offset, story in enumerate(stories, start=1)
                    ]
                    pages = cast(list[dict[str, Any]], metadata["pages"])
                    pages.append(
                        {
                            "endpoint": "story_list",
                            "page_number": page_number,
                            "elapsed_ms": round((time.monotonic() - started) * 1000),
                            "returned": len(stories),
                            "has_next_token": next_token is not None,
                        }
                    )
                    persist_search_page(
                        connection,
                        window_id=window_id,
                        page_number=page_number,
                        stories=normalized,
                        next_pagination_token=next_token,
                        window_raw_json=canonical_json(metadata),
                    )
                    token = next_token
                    if token is None:
                        mark_search_window_complete(
                            connection, window_id, raw_json=canonical_json(metadata)
                        )
                        succeeded += 1
                        break
                    page_number += 1
            except MediaCloudError as exc:
                mark_search_window_failed(
                    connection,
                    window_id,
                    status="failed",
                    error=str(exc),
                    raw_json=canonical_json(metadata),
                )
                failed += 1
    except MediaCloudError as exc:
        complete_pipeline_run(connection, run_id, status="failed", error=str(exc))
        raise
    complete_pipeline_run(connection, run_id, status="completed")
    return StageSummary(topic_name, len(windows), succeeded, skipped, failed)


REVIEW_COLUMNS = (
    "window_start",
    "window_end",
    "window_status",
    "pagination_completed",
    "page_number",
    "result_rank",
    "story_id",
    "title",
    "url",
    "domain",
    "publish_date",
    "indexed_at",
    "media_name",
    "media_url",
    "language",
    "first_seen_at",
    "last_seen_at",
)


def export_search_review(
    config: AppConfig,
    topic_name: str,
    connection: sqlite3.Connection,
    *,
    output: str | Path,
) -> SearchReviewSummary:
    """Export deterministic metadata rows and return reconciliation statistics."""
    _topic(config, topic_name)
    destination = Path(output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(f"{destination.suffix}.tmp")
    rows = search_review_rows(connection, topic_name)
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=REVIEW_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))
    temporary.replace(destination)

    stats = search_review_stats(connection, topic_name)
    windows = stats["window_count"]
    completed_windows = stats["completed_window_count"]
    hits = stats["hit_count"]
    distinct_stories = stats["story_count"]
    return SearchReviewSummary(
        topic=topic_name,
        windows=windows,
        completed_windows=completed_windows,
        incomplete_windows=windows - completed_windows,
        hits=hits,
        distinct_stories=distinct_stories,
        duplicate_hits=hits - distinct_stories,
        output_path=destination,
    )
