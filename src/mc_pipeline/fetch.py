"""Bounded, robots-aware full-text acquisition for QC-passing stories."""

from __future__ import annotations

import csv
import gzip
import os
import platform as python_platform
import random
import sqlite3
import tempfile
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from html.parser import HTMLParser
from importlib import import_module
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit
from urllib.robotparser import RobotFileParser

import requests
from requests import RequestException
from requests.adapters import HTTPAdapter

from .config import AppConfig
from .db import (
    MAX_FETCH_REVIEW_LIMIT,
    complete_pipeline_run,
    fetch_review_rows,
    start_pipeline_run,
    transaction,
)
from .errors import ConfigError, FetchError
from .identity import canonical_json, sha256_hex
from .ratelimit import IntervalSampler, PerDomainThrottle, TokenBucket, parse_retry_after
from .stage import FetchReviewSummary, StageSummary, git_revision, package_version

DEFAULT_MAX_REDIRECTS = 5
DEFAULT_MAX_RESPONSE_BYTES = 5 * 1024 * 1024
RAW_HTML_DIR = Path("data/raw_html")

FETCH_REVIEW_COLUMNS = (
    "topic",
    "story_id",
    "title",
    "url",
    "domain",
    "publish_date",
    "media_name",
    "fetch_status",
    "source",
    "http_status",
    "final_url",
    "extractor",
    "text",
    "text_chars",
    "attempts",
    "error",
    "fetched_at",
)

Clock = Callable[[], float]
Sleeper = Callable[[float], None]


@dataclass(frozen=True)
class FetchProgress:
    """One observable fetch-stage outcome for interactive reporters and tests."""

    processed: int
    total: int
    story_id: str
    domain: str
    status: str
    succeeded: int
    failed: int


class ProgressReporter(Protocol):
    """Receive an outcome after each selected story is handled."""

    def __call__(self, update: FetchProgress) -> None: ...


@dataclass(frozen=True)
class _RobotsResult:
    allowed: bool | None
    status: str | None
    http_status: int | None
    error: str | None


@dataclass(frozen=True)
class _FetchAttempt:
    source: str
    request_url: str | None
    final_url: str | None
    http_status: int | None
    elapsed_ms: int | None
    retry_after_s: float | None
    extractor: str | None
    text_chars: int | None
    error: str | None
    raw_metadata: Mapping[str, Any]


class _ParagraphParser(HTMLParser):
    """Small dependency-free fallback when BeautifulSoup is unavailable."""

    def __init__(self) -> None:
        super().__init__()
        self._in_paragraph = False
        self._parts: list[str] = []
        self._paragraphs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "p":
            self._in_paragraph = True
            self._parts = []

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "p" and self._in_paragraph:
            paragraph = " ".join("".join(self._parts).split())
            if paragraph:
                self._paragraphs.append(paragraph)
            self._parts = []
            self._in_paragraph = False

    def handle_data(self, data: str) -> None:
        if self._in_paragraph:
            self._parts.append(data)

    def text(self) -> str:
        return "\n\n".join(self._paragraphs)


def _topic(config: AppConfig, topic_name: str) -> Any:
    try:
        return config.topics[topic_name]
    except KeyError as exc:
        raise ConfigError(f"Unknown topic: {topic_name}") from exc


def export_fetch_review(
    config: AppConfig,
    topic_name: str,
    connection: sqlite3.Connection,
    *,
    limit: int,
    output: str | Path,
) -> FetchReviewSummary:
    """Export a bounded, read-only review of persisted fetch outcomes."""
    _topic(config, topic_name)
    valid_limit = isinstance(limit, int) and not isinstance(limit, bool)
    if not valid_limit or not 1 <= limit <= MAX_FETCH_REVIEW_LIMIT:
        raise ConfigError(f"limit must be between 1 and {MAX_FETCH_REVIEW_LIMIT}")

    destination = Path(output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    rows = fetch_review_rows(connection, topic_name, limit)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            newline="",
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            writer = csv.DictWriter(handle, fieldnames=FETCH_REVIEW_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow(dict(row))
        temporary_path.replace(destination)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)

    return FetchReviewSummary(
        topic=topic_name,
        limit=limit,
        exported=len(rows),
        output_path=destination,
    )


def _configured_int(fetch_config: Any, name: str, default: int) -> int:
    value = getattr(fetch_config, name, default)
    if not isinstance(value, int) or value <= 0:
        raise ConfigError(f"fetch.{name} must be a positive integer")
    return value


def _safe_component(value: str) -> str:
    result = "".join(
        character if character.isalnum() or character in "-_" else "_" for character in value
    )
    return result or "unknown"


def _valid_url(value: str | None) -> tuple[str, str] | None:
    if not value:
        return None
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return parsed.geturl(), parsed.netloc.lower()


def _priority_key(row: sqlite3.Row, title_terms: tuple[str, ...]) -> tuple[int, int, str, str, str]:
    title = (row["title"] or "").casefold()
    matching_terms = [index for index, term in enumerate(title_terms) if term in title]
    if matching_terms:
        return (
            0,
            matching_terms[0],
            str(row["publish_date"] or ""),
            str(row["domain"] or ""),
            str(row["story_id"]),
        )
    return (
        1,
        len(title_terms),
        str(row["publish_date"] or ""),
        str(row["domain"] or ""),
        str(row["story_id"]),
    )


def _pending_rows(connection: sqlite3.Connection, topic_name: str) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT DISTINCT stories.story_id, stories.title, stories.url, stories.domain,
               stories.publish_date, stories.mc_text, articles.fetch_status,
               articles.raw_html_path
        FROM search_hits
        JOIN search_windows ON search_windows.id = search_hits.search_window_id
        JOIN story_topic_state
          ON story_topic_state.story_id = search_hits.story_id
         AND story_topic_state.topic = search_windows.topic
        JOIN stories ON stories.story_id = search_hits.story_id
        LEFT JOIN articles ON articles.story_id = stories.story_id
        WHERE search_windows.topic = ?
          AND story_topic_state.qc_status = 'ok'
          AND (articles.id IS NULL OR articles.fetch_status <> 'ok')
        ORDER BY stories.story_id ASC
        """,
        (topic_name,),
    ).fetchall()


def _topic_fetch_counts(connection: sqlite3.Connection, topic_name: str) -> dict[str, int]:
    row = connection.execute(
        """
        SELECT
            COUNT(DISTINCT stories.story_id) AS qc_passing,
            COUNT(DISTINCT CASE WHEN articles.fetch_status = 'ok' THEN stories.story_id END)
                AS cached_ok,
            COUNT(DISTINCT CASE
                WHEN articles.id IS NOT NULL AND articles.fetch_status <> 'ok'
                THEN stories.story_id
            END) AS terminal
        FROM search_hits
        JOIN search_windows ON search_windows.id = search_hits.search_window_id
        JOIN story_topic_state
          ON story_topic_state.story_id = search_hits.story_id
         AND story_topic_state.topic = search_windows.topic
        JOIN stories ON stories.story_id = search_hits.story_id
        LEFT JOIN articles ON articles.story_id = stories.story_id
        WHERE search_windows.topic = ?
          AND story_topic_state.qc_status = 'ok'
        """,
        (topic_name,),
    ).fetchone()
    return {
        "qc_passing": int(row["qc_passing"]),
        "cached_ok": int(row["cached_ok"]),
        "terminal": int(row["terminal"]),
    }


def _new_session() -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=0)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _response_bytes(response: Any, maximum: int) -> bytes:
    chunks: list[bytes] = []
    size = 0
    iterator = getattr(response, "iter_content", None)
    if callable(iterator):
        for chunk in iterator(chunk_size=64 * 1024):
            if not chunk:
                continue
            size += len(chunk)
            if size > maximum:
                raise ValueError(f"response exceeds {maximum} byte limit")
            chunks.append(chunk)
        return b"".join(chunks)

    content = bytes(getattr(response, "content", b""))
    if len(content) > maximum:
        raise ValueError(f"response exceeds {maximum} byte limit")
    return content


def _response_text(response: Any, maximum: int) -> str:
    content = _response_bytes(response, maximum)
    encoding = getattr(response, "encoding", None) or "utf-8"
    return content.decode(encoding, errors="replace")


def _close_response(response: Any) -> None:
    close = getattr(response, "close", None)
    if callable(close):
        close()


def _get_robots(
    *,
    session: requests.Session,
    url: str,
    domain: str,
    user_agent: str,
    timeout_s: float,
    max_response_bytes: int,
    cache: dict[str, _RobotsResult],
    global_limiter: TokenBucket,
    domain_throttle: PerDomainThrottle,
    clock: Clock,
) -> tuple[_RobotsResult, _FetchAttempt | None]:
    parsed = urlsplit(url)
    origin = f"{parsed.scheme}://{parsed.netloc.lower()}"
    cached = cache.get(origin)
    if cached is not None:
        return cached, None

    robots_url = f"{origin}/robots.txt"
    global_limiter.acquire()
    domain_throttle.wait(domain)
    started = clock()
    response: Any | None = None
    try:
        response = session.get(
            robots_url,
            headers={"User-Agent": user_agent},
            timeout=timeout_s,
            allow_redirects=True,
            stream=True,
        )
    except RequestException as exc:
        result = _RobotsResult(None, "robots_unavailable", None, str(exc))
        attempt = _FetchAttempt(
            "robots",
            robots_url,
            None,
            None,
            round((clock() - started) * 1000),
            None,
            None,
            None,
            str(exc),
            {},
        )
    else:
        try:
            http_status = int(response.status_code)
            final_url = str(getattr(response, "url", robots_url))
            retry_after_s = parse_retry_after(getattr(response, "headers", {}).get("Retry-After"))
            if not 200 <= http_status < 300:
                result = _RobotsResult(None, "robots_unavailable", http_status, None)
            else:
                parser = RobotFileParser()
                parser.parse(_response_text(response, max_response_bytes).splitlines())
                result = _RobotsResult(parser.can_fetch(user_agent, url), None, http_status, None)
            attempt = _FetchAttempt(
                "robots",
                robots_url,
                final_url,
                http_status,
                round((clock() - started) * 1000),
                retry_after_s,
                None,
                None,
                result.error,
                {"content_type": getattr(response, "headers", {}).get("Content-Type")},
            )
        except (UnicodeError, ValueError, RequestException) as exc:
            result = _RobotsResult(None, "robots_unavailable", None, str(exc))
            attempt = _FetchAttempt(
                "robots",
                robots_url,
                str(getattr(response, "url", robots_url)),
                int(getattr(response, "status_code", 0)) or None,
                round((clock() - started) * 1000),
                None,
                None,
                None,
                str(exc),
                {},
            )
        finally:
            _close_response(response)
    cache[origin] = result
    return result, attempt


def _paragraph_text(html: str) -> str:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        parser = _ParagraphParser()
        parser.feed(html)
        return parser.text()

    soup = BeautifulSoup(html, "html.parser")
    return "\n\n".join(
        paragraph.get_text(" ", strip=True)
        for paragraph in soup.find_all("p")
        if paragraph.get_text(strip=True)
    )


def _extract_text(html: str, minimum_chars: int) -> tuple[str | None, str | None]:
    try:
        trafilatura = import_module("trafilatura")
    except ImportError:
        trafilatura = None
    if trafilatura is not None:
        text = trafilatura.extract(html)
        if text is not None and len(text) >= minimum_chars:
            return text, "trafilatura"

    try:
        document_class = import_module("readability").__dict__["Document"]
    except (ImportError, KeyError):
        document_class = None
    if document_class is not None:
        text = _paragraph_text(document_class(html).summary())
        if len(text) >= minimum_chars:
            return text, "readability-lxml"

    text = _paragraph_text(html)
    if len(text) >= minimum_chars:
        return text, "beautifulsoup"
    return None, None


def _save_raw_html(story_id: str, html: bytes) -> str:
    safe_story_id = _safe_component(story_id)
    output_dir = RAW_HTML_DIR / safe_story_id[:2]
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{safe_story_id}.html.gz"
    file_descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{output_path.name}.", dir=output_dir
    )
    try:
        with (
            os.fdopen(file_descriptor, "wb") as temporary_file,
            gzip.GzipFile(fileobj=temporary_file, mode="wb") as compressed,
        ):
            compressed.write(html)
        os.replace(temporary_name, output_path)
    except BaseException:
        Path(temporary_name).unlink(missing_ok=True)
        raise
    return str(output_path)


def _persist_outcome(
    connection: sqlite3.Connection,
    *,
    story_id: str,
    status: str,
    source: str,
    request_url: str | None,
    final_url: str | None,
    http_status: int | None,
    elapsed_ms: int | None,
    extractor: str | None,
    text: str | None,
    error: str | None,
    raw_html_path: str | None,
    raw_metadata: Mapping[str, Any],
    retry_after_s: float | None = None,
    record_attempt: bool = True,
    preceding_attempts: tuple[_FetchAttempt, ...] = (),
) -> None:
    text_chars = len(text) if text is not None else None
    bounded_error = error[:1000] if error is not None else None
    attempts = list(preceding_attempts)
    if record_attempt:
        attempts.append(
            _FetchAttempt(
                source,
                request_url,
                final_url,
                http_status,
                elapsed_ms,
                retry_after_s,
                extractor,
                text_chars,
                bounded_error,
                raw_metadata,
            )
        )
    with transaction(connection):
        existing = connection.execute(
            "SELECT id, attempts FROM articles WHERE story_id = ?", (story_id,)
        ).fetchone()
        previous_attempts = int(existing["attempts"]) if existing is not None else 0
        updated_attempts = previous_attempts + len(attempts)
        connection.execute(
            """
            INSERT INTO articles(
                story_id, fetch_status, source, http_status, final_url, extractor, text,
                text_chars, attempts, error, raw_html_path, raw_json, fetched_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(story_id) DO UPDATE SET
                fetch_status = excluded.fetch_status,
                source = excluded.source,
                http_status = excluded.http_status,
                final_url = excluded.final_url,
                extractor = excluded.extractor,
                text = excluded.text,
                text_chars = excluded.text_chars,
                attempts = excluded.attempts,
                error = excluded.error,
                raw_html_path = COALESCE(excluded.raw_html_path, articles.raw_html_path),
                raw_json = excluded.raw_json,
                fetched_at = CURRENT_TIMESTAMP
            """,
            (
                story_id,
                status,
                source,
                http_status,
                final_url,
                extractor,
                text,
                text_chars,
                updated_attempts,
                bounded_error,
                raw_html_path,
                canonical_json(dict(raw_metadata)),
            ),
        )
        article = connection.execute(
            "SELECT id FROM articles WHERE story_id = ?", (story_id,)
        ).fetchone()
        if article is None:
            raise RuntimeError(f"Failed to persist article for story {story_id}")
        for offset, attempt in enumerate(attempts, start=1):
            connection.execute(
                """
                INSERT INTO fetch_attempts(
                    article_id, attempt_number, source, request_url, final_url, http_status,
                    elapsed_ms, retry_after_s, extractor, text_chars, error, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    article["id"],
                    previous_attempts + offset,
                    attempt.source,
                    attempt.request_url,
                    attempt.final_url,
                    attempt.http_status,
                    attempt.elapsed_ms,
                    attempt.retry_after_s,
                    attempt.extractor,
                    attempt.text_chars,
                    attempt.error,
                    canonical_json(dict(attempt.raw_metadata)),
                ),
            )


def _request_detail(row: sqlite3.Row, source: str) -> str:
    return canonical_json(
        {
            "endpoint": "article_fetch",
            "source": source,
            "story_id": row["story_id"],
            "domain": row["domain"],
            "publish_date": row["publish_date"],
            "url": row["url"],
        }
    )


def _validate_live_fetch_config(config: AppConfig) -> None:
    user_agent = config.fetch.user_agent.casefold()
    if "mailto:" not in user_agent or any(
        placeholder in user_agent
        for placeholder in ("you@example.org", "example.com", "example.net")
    ):
        raise ConfigError(
            "fetch.user_agent must contain a real mailto contact before live fetching"
        )
    if config.fetch.max_retries != 0:
        raise ConfigError("fetch.max_retries must remain 0")
    if config.fetch.wayback_fallback:
        raise ConfigError("fetch.wayback_fallback must remain false")


def _fetch_provenance(config: AppConfig, topic_name: str) -> tuple[str, str, str, str]:
    payload = {
        "fetch": config.fetch.model_dump(mode="json"),
        "topic": config.topics[topic_name].model_dump(mode="json"),
    }
    config_json = canonical_json(payload)
    versions_json = canonical_json(
        {
            "python": python_platform.python_version(),
            "media-cloud-pipeline": package_version("media-cloud-pipeline"),
            "requests": package_version("requests"),
            "trafilatura": package_version("trafilatura"),
            "readability-lxml": package_version("readability-lxml"),
            "beautifulsoup4": package_version("beautifulsoup4"),
        }
    )
    return sha256_hex(payload), config_json, git_revision(), versions_json


def _stored_html_text(
    raw_html_path: str | None, minimum_chars: int
) -> tuple[bool, str | None, str | None]:
    if not raw_html_path:
        return False, None, None
    path = Path(raw_html_path)
    if not path.is_file():
        return False, None, None
    try:
        with gzip.open(path, "rb") as compressed:
            html = compressed.read().decode("utf-8", errors="replace")
    except OSError as exc:
        raise FetchError(f"Failed to read stored HTML for re-extraction: {path}") from exc
    text, extractor = _extract_text(html, minimum_chars)
    return True, text, extractor


def fetch_topic(
    config: AppConfig,
    topic_name: str,
    connection: sqlite3.Connection,
    *,
    limit: int | None = None,
    dry_run: bool = False,
    progress: ProgressReporter | None = None,
    _session: requests.Session | None = None,
    _clock: Clock = time.monotonic,
    _sleeper: Sleeper = time.sleep,
    _random_uniform: IntervalSampler = random.uniform,
) -> StageSummary:
    """Acquire and cache text for QC-passing stories reachable from one topic.

    The private injection arguments keep production calls small while allowing the
    offline tests to provide a fake HTTP session and monotonic clock.
    """
    if limit is not None and limit <= 0:
        raise ConfigError("limit must be positive")

    topic = _topic(config, topic_name)
    rows = _pending_rows(connection, topic_name)
    title_terms = tuple(
        term.casefold() for term in getattr(topic, "fetch_priority_title_terms", []) if term
    )
    rows.sort(key=lambda row: _priority_key(row, title_terms))
    if limit is not None:
        rows = rows[:limit]

    minimum_chars = config.fetch.min_text_chars
    if dry_run:
        topic_counts = _topic_fetch_counts(connection, topic_name)
        preview = tuple(
            _request_detail(
                row,
                "mediacloud"
                if len(row["mc_text"] or "") >= minimum_chars
                else "stored_html"
                if row["raw_html_path"] and Path(row["raw_html_path"]).is_file()
                else "url",
            )
            for row in rows[:20]
        )
        source_counts = {"mediacloud": 0, "stored_html": 0, "url": 0}
        for row in rows:
            if len(row["mc_text"] or "") >= minimum_chars:
                source_counts["mediacloud"] += 1
            elif row["raw_html_path"] and Path(row["raw_html_path"]).is_file():
                source_counts["stored_html"] += 1
            else:
                source_counts["url"] += 1
        details = (
            canonical_json(
                {
                    "dry_run": True,
                    **topic_counts,
                    "selected": len(rows),
                    "previewed": len(preview),
                    "omitted": max(0, len(rows) - len(preview)),
                    "planned_sources": source_counts,
                }
            ),
            *preview,
        )
        if progress is not None:
            for index, row in enumerate(rows, start=1):
                progress(
                    FetchProgress(
                        index,
                        len(rows),
                        str(row["story_id"]),
                        str(row["domain"] or "unknown"),
                        "planned",
                        0,
                        0,
                    )
                )
        return StageSummary(topic_name, len(rows), 0, len(rows), 0, details)

    _validate_live_fetch_config(config)
    config_hash, config_json, revision, versions_json = _fetch_provenance(config, topic_name)
    run_id = start_pipeline_run(
        connection,
        run_uuid=str(uuid.uuid4()),
        topic=topic_name,
        stage="fetch",
        config_hash=config_hash,
        config_json=config_json,
        git_revision=revision,
        package_versions_json=versions_json,
        raw_json=canonical_json({"command": "fetch", "limit": limit}),
    )
    owns_session = _session is None
    session = _session if _session is not None else _new_session()
    previous_redirect_limit = getattr(session, "max_redirects", None)
    session.max_redirects = _configured_int(config.fetch, "max_redirects", DEFAULT_MAX_REDIRECTS)
    max_response_bytes = _configured_int(
        config.fetch, "max_response_bytes", DEFAULT_MAX_RESPONSE_BYTES
    )
    global_limiter = TokenBucket(
        config.fetch.global_max_rps * 60,
        capacity=1,
        clock=_clock,
        sleeper=_sleeper,
    )
    domain_throttle = PerDomainThrottle(
        config.fetch.per_domain_delay_s,
        clock=_clock,
        sleeper=_sleeper,
        random_uniform=_random_uniform,
    )
    robots_cache: dict[str, _RobotsResult] = {}
    succeeded = failed = 0
    try:
        for index, row in enumerate(rows, start=1):
            story_id = str(row["story_id"])
            mc_text = row["mc_text"]
            if mc_text is not None and len(mc_text) >= minimum_chars:
                _persist_outcome(
                    connection,
                    story_id=story_id,
                    status="ok",
                    source="mediacloud",
                    request_url=None,
                    final_url=row["url"],
                    http_status=None,
                    elapsed_ms=0,
                    extractor="mediacloud",
                    text=mc_text,
                    error=None,
                    raw_html_path=None,
                    raw_metadata={"source": "mediacloud"},
                    record_attempt=False,
                )
                status = "ok"
            else:
                has_stored_html, stored_text, stored_extractor = _stored_html_text(
                    row["raw_html_path"], minimum_chars
                )
                if has_stored_html:
                    _persist_outcome(
                        connection,
                        story_id=story_id,
                        status="ok" if stored_text is not None else "too_short",
                        source="stored_html",
                        request_url=None,
                        final_url=row["url"],
                        http_status=None,
                        elapsed_ms=0,
                        extractor=stored_extractor,
                        text=stored_text,
                        error=(
                            None
                            if stored_text is not None
                            else "Stored HTML did not reach min_text_chars"
                        ),
                        raw_html_path=row["raw_html_path"],
                        raw_metadata={"source": "stored_html"},
                        record_attempt=False,
                    )
                    status = "ok" if stored_text is not None else "too_short"
                else:
                    status = _fetch_url(
                        connection=connection,
                        story_id=story_id,
                        url=row["url"],
                        session=session,
                        fetch_config=config.fetch,
                        max_response_bytes=max_response_bytes,
                        robots_cache=robots_cache,
                        global_limiter=global_limiter,
                        domain_throttle=domain_throttle,
                        clock=_clock,
                    )
            if status == "ok":
                succeeded += 1
            else:
                failed += 1
            if progress is not None:
                progress(
                    FetchProgress(
                        index,
                        len(rows),
                        story_id,
                        str(row["domain"] or "unknown"),
                        status,
                        succeeded,
                        failed,
                    )
                )
    except BaseException as exc:
        complete_pipeline_run(connection, run_id, status="failed", error=str(exc)[:1000])
        raise
    finally:
        if previous_redirect_limit is not None:
            session.max_redirects = previous_redirect_limit
        if owns_session:
            session.close()

    complete_pipeline_run(connection, run_id, status="completed")
    return StageSummary(topic_name, len(rows), succeeded, 0, failed)


def _fetch_url(
    *,
    connection: sqlite3.Connection,
    story_id: str,
    url: str | None,
    session: requests.Session,
    fetch_config: Any,
    max_response_bytes: int,
    robots_cache: dict[str, _RobotsResult],
    global_limiter: TokenBucket,
    domain_throttle: PerDomainThrottle,
    clock: Clock,
) -> str:
    valid_url = _valid_url(url)
    if valid_url is None:
        _persist_outcome(
            connection,
            story_id=story_id,
            status="invalid_url",
            source="url",
            request_url=url,
            final_url=None,
            http_status=None,
            elapsed_ms=None,
            extractor=None,
            text=None,
            error="URL must be absolute HTTP or HTTPS",
            raw_html_path=None,
            raw_metadata={},
            record_attempt=False,
        )
        return "invalid_url"

    request_url, domain = valid_url
    robots_metadata: dict[str, Any] = {}
    preceding_attempts: tuple[_FetchAttempt, ...] = ()
    if fetch_config.respect_robots:
        robots, robots_attempt = _get_robots(
            session=session,
            url=request_url,
            domain=domain,
            user_agent=fetch_config.user_agent,
            timeout_s=fetch_config.timeout_s,
            max_response_bytes=max_response_bytes,
            cache=robots_cache,
            global_limiter=global_limiter,
            domain_throttle=domain_throttle,
            clock=clock,
        )
        preceding_attempts = () if robots_attempt is None else (robots_attempt,)
        robots_metadata = {
            "robots_http_status": robots.http_status,
            "robots_error": robots.error,
        }
        if robots.status is not None:
            _persist_outcome(
                connection,
                story_id=story_id,
                status=robots.status,
                source="url",
                request_url=request_url,
                final_url=None,
                http_status=robots.http_status,
                elapsed_ms=None,
                extractor=None,
                text=None,
                error=robots.error,
                raw_html_path=None,
                raw_metadata=robots_metadata,
                record_attempt=False,
                preceding_attempts=preceding_attempts,
            )
            return robots.status
        if not robots.allowed:
            _persist_outcome(
                connection,
                story_id=story_id,
                status="robots_denied",
                source="url",
                request_url=request_url,
                final_url=None,
                http_status=robots.http_status,
                elapsed_ms=None,
                extractor=None,
                text=None,
                error="robots.txt disallows this user agent",
                raw_html_path=None,
                raw_metadata=robots_metadata,
                record_attempt=False,
                preceding_attempts=preceding_attempts,
            )
            return "robots_denied"

    global_limiter.acquire()
    domain_throttle.wait(domain)
    started = clock()
    response: Any | None = None
    try:
        response = session.get(
            request_url,
            headers={"User-Agent": fetch_config.user_agent},
            timeout=fetch_config.timeout_s,
            allow_redirects=True,
            stream=True,
        )
        elapsed_ms = round((clock() - started) * 1000)
        http_status = int(response.status_code)
        final_url = str(getattr(response, "url", request_url))
        retry_after_s = parse_retry_after(getattr(response, "headers", {}).get("Retry-After"))
        metadata = {
            **robots_metadata,
            "content_type": getattr(response, "headers", {}).get("Content-Type"),
            "response_bytes": None,
        }
        if http_status in {401, 403, 429, 451}:
            status, error = "blocked", f"HTTP {http_status}"
        elif not 200 <= http_status < 300:
            status, error = "fetch_failed", f"HTTP {http_status}"
        else:
            content_type = getattr(response, "headers", {}).get("Content-Type", "")
            media_type = content_type.split(";", 1)[0].strip().lower()
            if media_type not in {"text/html", "application/xhtml+xml"}:
                status, error = (
                    "unsupported_content",
                    f"Unsupported content type: {content_type or 'missing'}",
                )
            else:
                html_bytes = _response_bytes(response, max_response_bytes)
                metadata["response_bytes"] = len(html_bytes)
                raw_html_path = None
                if fetch_config.save_raw_html:
                    raw_html_path = _save_raw_html(story_id, html_bytes)
                html = html_bytes.decode(
                    getattr(response, "encoding", None) or "utf-8", errors="replace"
                )
                text, extractor = _extract_text(html, fetch_config.min_text_chars)
                if text is None:
                    status, error = "too_short", "No extractor reached min_text_chars"
                else:
                    status, error = "ok", None
                _persist_outcome(
                    connection,
                    story_id=story_id,
                    status=status,
                    source="url",
                    request_url=request_url,
                    final_url=final_url,
                    http_status=http_status,
                    elapsed_ms=elapsed_ms,
                    extractor=extractor,
                    text=text,
                    error=error,
                    raw_html_path=raw_html_path,
                    raw_metadata=metadata,
                    retry_after_s=retry_after_s,
                    preceding_attempts=preceding_attempts,
                )
                return status
        _persist_outcome(
            connection,
            story_id=story_id,
            status=status,
            source="url",
            request_url=request_url,
            final_url=final_url,
            http_status=http_status,
            elapsed_ms=elapsed_ms,
            extractor=None,
            text=None,
            error=error,
            raw_html_path=None,
            raw_metadata=metadata,
            retry_after_s=retry_after_s,
            preceding_attempts=preceding_attempts,
        )
        return status
    except (RequestException, UnicodeError, ValueError) as exc:
        elapsed_ms = round((clock() - started) * 1000)
        _persist_outcome(
            connection,
            story_id=story_id,
            status="fetch_failed",
            source="url",
            request_url=request_url,
            final_url=None,
            http_status=None,
            elapsed_ms=elapsed_ms,
            extractor=None,
            text=None,
            error=str(exc),
            raw_html_path=None,
            raw_metadata=robots_metadata,
            preceding_attempts=preceding_attempts,
        )
        return "fetch_failed"
    finally:
        if response is not None:
            _close_response(response)
