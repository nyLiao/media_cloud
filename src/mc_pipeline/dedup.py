"""Deterministic topic-scoped quality control and exact deduplication."""

from __future__ import annotations

import csv
import sqlite3
import uuid
from collections.abc import Iterable, Mapping, Sequence
from contextlib import suppress
from datetime import date
from pathlib import Path
from typing import Literal, TypedDict, cast
from urllib.parse import parse_qsl, urlencode, urlsplit

from .config import AppConfig, TopicConfig
from .db import (
    complete_pipeline_run,
    dedup_review_rows,
    dedup_review_stats,
    persist_topic_story_states,
    start_pipeline_run,
    topic_reachable_stories,
)
from .errors import ConfigError
from .stage import DedupReviewSummary, StageSummary, build_provenance


class TopicState(TypedDict):
    story_id: str
    url_norm: str | None
    title_norm: str | None
    qc_status: str
    qc_reason: str | None
    dup_of_story_id: str | None


OUTLET_SUFFIXES = frozenset({"cbc news", "the globe and mail", "national post"})

DEDUP_REVIEW_COLUMNS = (
    "story_id",
    "qc_status",
    "qc_reason",
    "dup_of_story_id",
    "duplicate_of_title",
    "duplicate_of_url",
    "duplicate_of_publish_date",
    "title",
    "url",
    "url_norm",
    "title_norm",
    "domain",
    "publish_date",
    "media_name",
    "media_url",
    "language",
    "indexed_at",
    "first_seen_at",
    "last_seen_at",
    "decided_at",
)


def normalize_url(url: str | None) -> str | None:
    """Normalize a URL for deterministic exact matching."""
    if not url or not url.strip():
        return None
    try:
        parsed = urlsplit(url.strip())
        if not parsed.hostname:
            return None
        host = parsed.hostname.casefold()
        if host.startswith("www."):
            host = host[4:]
        if parsed.port is not None:
            host = f"{host}:{parsed.port}"
    except ValueError:
        return None
    path = parsed.path.rstrip("/")
    query = sorted(
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if not (key.casefold().startswith("utm_") or key.casefold() == "fbclid")
    )
    suffix = f"?{urlencode(query, doseq=True)}" if query else ""
    return f"{host}{path}{suffix}"


def normalize_title(title: str | None, media_name: str | None = None) -> str | None:
    """Normalize a title after removing common outlet suffixes."""
    if not title or not title.strip():
        return None
    value = title.strip()
    outlet_suffixes = set(OUTLET_SUFFIXES)
    if media_name and media_name.strip():
        outlet_suffixes.add(media_name.strip().casefold())
    for separator in (" | ", " - ", " — "):
        if separator in value:
            candidate, suffix = value.rsplit(separator, maxsplit=1)
            if suffix.strip().casefold() in outlet_suffixes:
                value = candidate
                break
    normalized = "".join(
        character if character.isalnum() else " " for character in value.casefold()
    )
    collapsed = " ".join(normalized.split())
    return collapsed or None


def _topic(config: AppConfig, topic_name: str) -> TopicConfig:
    try:
        return config.topics[topic_name]
    except KeyError as exc:
        raise ConfigError(f"Unknown topic: {topic_name}") from exc


def _publish_date(value: str | None) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _domain(url_norm: str | None) -> str | None:
    if url_norm is None:
        return None
    return url_norm.split("/", maxsplit=1)[0].split(":", maxsplit=1)[0]


def _is_allowed_domain(domain: str | None, allowlist: Iterable[str]) -> bool:
    normalized_allowlist = {
        candidate.casefold().removeprefix("www.").rstrip("/") for candidate in allowlist
    }
    if not normalized_allowlist:
        return True
    if domain is None:
        return False
    return any(
        domain == candidate or domain.endswith(f".{candidate}")
        for candidate in normalized_allowlist
    )


def _matching_title_term(title_norm: str | None, excluded_terms: Iterable[str]) -> str | None:
    """Return the first configured normalized term matched on complete title tokens."""
    if title_norm is None:
        return None
    title_tokens = title_norm.split()
    for term in excluded_terms:
        term_tokens = term.split()
        width = len(term_tokens)
        if any(
            title_tokens[index : index + width] == term_tokens for index in range(len(title_tokens))
        ):
            return term
    return None


def _qc_reason(
    row: sqlite3.Row,
    topic: TopicConfig,
    *,
    url_norm: str | None,
    title_norm: str | None,
) -> str | None:
    language = row["language"]
    allowed_languages = {item.casefold() for item in topic.languages}
    if not isinstance(language, str) or language.casefold() not in allowed_languages:
        return "bad_lang"

    publish_date = _publish_date(row["publish_date"])
    if publish_date is None or not topic.start_date <= publish_date <= topic.end_date:
        return "out_of_range"

    url = row["url"]
    if url_norm is None or (
        isinstance(url, str)
        and any(pattern.casefold() in url.casefold() for pattern in topic.qc.exclude_url_patterns)
    ):
        return "bad_url"

    title = row["title"]
    if not isinstance(title, str) or len(title.strip()) < topic.qc.min_title_chars:
        return "short_title"

    excluded_term = _matching_title_term(title_norm, topic.qc.exclude_title_terms)
    if excluded_term is not None:
        return f"sports_title:{excluded_term}"

    if not _is_allowed_domain(_domain(url_norm), topic.domain_allowlist):
        return "off_domain"
    return None


def _canonical(states: list[TopicState], publish_dates: dict[str, date]) -> TopicState:
    return min(
        states,
        key=lambda state: (publish_dates.get(state["story_id"], date.max), state["story_id"]),
    )


def _mark_duplicates(
    states: list[TopicState],
    publish_dates: dict[str, date],
    key: Literal["url_norm", "title_norm"],
    reason: Literal["duplicate_url", "duplicate_title"],
) -> None:
    groups: dict[str, list[TopicState]] = {}
    for state in states:
        value = state[key]
        if state["qc_status"] == "ok" and value is not None:
            groups.setdefault(value, []).append(state)
    for duplicates in groups.values():
        if len(duplicates) < 2:
            continue
        canonical = _canonical(duplicates, publish_dates)
        for duplicate in duplicates:
            if duplicate is canonical:
                continue
            duplicate["qc_status"] = "dup"
            duplicate["qc_reason"] = reason
            duplicate["dup_of_story_id"] = canonical["story_id"]


def _flatten_duplicate_targets(states: list[TopicState]) -> None:
    by_story_id = {state["story_id"]: state for state in states}
    for state in states:
        target_id = state["dup_of_story_id"]
        while target_id is not None:
            target = by_story_id[target_id]
            next_target_id = target["dup_of_story_id"]
            if next_target_id is None:
                break
            target_id = next_target_id
        state["dup_of_story_id"] = target_id


def export_dedup_review(
    config: AppConfig,
    topic_name: str,
    connection: sqlite3.Connection,
    *,
    output: str | Path,
) -> DedupReviewSummary:
    """Export deterministic Stage 2 outcomes and return reconciliation statistics."""
    _topic(config, topic_name)
    destination = Path(output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(f"{destination.suffix}.tmp")
    rows = dedup_review_rows(connection, topic_name)
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=DEDUP_REVIEW_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))
    temporary.replace(destination)

    stats = dedup_review_stats(connection, topic_name)
    stories = stats["story_count"]
    decided = stats["decided_story_count"]
    return DedupReviewSummary(
        topic=topic_name,
        stories=stories,
        decided=decided,
        accepted=stats["accepted_story_count"],
        duplicates=stats["duplicate_story_count"],
        rejected=stats["rejected_story_count"],
        undecided=stories - decided,
        duplicate_urls=stats["duplicate_url_count"],
        duplicate_titles=stats["duplicate_title_count"],
        sports_titles=stats["sports_title_count"],
        output_path=destination,
    )


def deduplicate_topic(
    config: AppConfig, topic_name: str, connection: sqlite3.Connection
) -> StageSummary:
    """QC and deduplicate all distinct stories reachable from one topic."""
    topic = _topic(config, topic_name)
    rows = topic_reachable_stories(connection, topic_name)
    states: list[TopicState] = []
    publish_dates: dict[str, date] = {}
    for row in rows:
        url_norm = normalize_url(row["url"])
        title_norm = normalize_title(row["title"], row["media_name"])
        reason = _qc_reason(row, topic, url_norm=url_norm, title_norm=title_norm)
        state: TopicState = {
            "story_id": row["story_id"],
            "url_norm": url_norm,
            "title_norm": title_norm,
            "qc_status": "sports_title"
            if reason and reason.startswith("sports_title:")
            else ("ok" if reason is None else reason),
            "qc_reason": reason,
            "dup_of_story_id": None,
        }
        states.append(state)
        publish_date = _publish_date(row["publish_date"])
        if publish_date is not None:
            publish_dates[state["story_id"]] = publish_date

    _mark_duplicates(states, publish_dates, "url_norm", "duplicate_url")
    _mark_duplicates(states, publish_dates, "title_norm", "duplicate_title")
    _flatten_duplicate_targets(states)

    config_hash, config_json, git_revision, versions_json = build_provenance(config, topic_name)
    run_id = start_pipeline_run(
        connection,
        run_uuid=str(uuid.uuid4()),
        topic=topic_name,
        stage="dedup",
        config_hash=config_hash,
        config_json=config_json,
        git_revision=git_revision,
        package_versions_json=versions_json,
    )
    try:
        persist_topic_story_states(
            connection,
            topic=topic_name,
            states=cast(Sequence[Mapping[str, str | None]], states),
        )
    except BaseException as exc:
        with suppress(Exception):
            complete_pipeline_run(connection, run_id, status="failed", error=str(exc))
        raise
    complete_pipeline_run(connection, run_id, status="completed")
    succeeded = sum(state["qc_status"] == "ok" for state in states)
    return StageSummary(topic_name, len(states), succeeded, len(states) - succeeded, 0)
