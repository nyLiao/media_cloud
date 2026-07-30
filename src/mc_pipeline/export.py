"""Deterministic CSV exports for stored pipeline results."""

from __future__ import annotations

import csv
import os
import sqlite3
import tempfile
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

from .config import AppConfig, TopicConfig
from .contracts import ARTICLE_AUDIT_COLUMNS, case_csv_columns
from .db import transaction
from .errors import ConfigError, ExportError
from .stage import StageSummary


def _topic(config: AppConfig, topic_name: str) -> TopicConfig:
    try:
        return config.topics[topic_name]
    except KeyError as exc:
        raise ConfigError(f"Unknown topic: {topic_name}") from exc


def _quoted_identifier(name: str) -> str:
    """Quote a config-derived SQLite identifier."""
    return f'"{name.replace(chr(34), chr(34) * 2)}"'


def _empty_for_null(value: Any) -> Any:
    return "" if value is None else value


def _write_csv(
    destination: Path, columns: Sequence[str], rows: Iterable[sqlite3.Row]
) -> tuple[Path, int]:
    descriptor, temporary_name = tempfile.mkstemp(
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=".tmp",
    )
    temporary = Path(temporary_name)
    count = 0
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns, lineterminator="\r\n")
            writer.writeheader()
            for row in rows:
                writer.writerow({column: _empty_for_null(row[column]) for column in columns})
                count += 1
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return temporary, count


def _case_query(topic: TopicConfig, stored_columns: set[str]) -> str:
    def expression(field_name: str) -> str:
        json_value = f"json_extract(cases.raw_json, '$.{field_name}')"
        if field_name not in stored_columns:
            return json_value
        return f"COALESCE({json_value}, cases.{_quoted_identifier(field_name)})"

    extraction_columns = ",\n               ".join(
        f"{expression(field.name)} AS {_quoted_identifier(field.name)}"
        for field in topic.extraction.fields
    )
    return f"""
        SELECT
            ? AS topic,
            cases.case_id,
            stories.story_id,
            stories.url,
            stories.domain,
            stories.publish_date,
            stories.media_name,
            stories.title,
            stories.media_name AS source_outlet,
            stories.url AS source_url,
            ? AS link_category,
            {extraction_columns},
            cases.evidence_verified,
            story_extractions.model,
            story_extractions.prompt_version,
            story_extractions.extracted_at
        FROM cases
        JOIN story_extractions ON story_extractions.id = cases.story_extraction_id
        JOIN stories ON stories.story_id = story_extractions.story_id
        WHERE story_extractions.topic = ?
          AND story_extractions.prompt_version = ?
          AND story_extractions.relevant = 1
        ORDER BY stories.publish_date ASC, stories.story_id ASC, cases.case_id ASC
    """


_ARTICLE_AUDIT_QUERY = """
    WITH reachable_stories AS (
        SELECT DISTINCT search_hits.story_id
        FROM search_hits
        JOIN search_windows ON search_windows.id = search_hits.search_window_id
        WHERE search_windows.topic = ?
    ), latest_extractions AS (
        SELECT story_id, relevant, reject_reason
        FROM (
            SELECT
                story_extractions.story_id,
                story_extractions.relevant,
                story_extractions.reject_reason,
                ROW_NUMBER() OVER (
                    PARTITION BY story_extractions.story_id
                    ORDER BY story_extractions.extracted_at DESC, story_extractions.id DESC
                ) AS row_number
            FROM story_extractions
            WHERE story_extractions.topic = ?
              AND story_extractions.prompt_version = ?
        )
        WHERE row_number = 1
    )
    SELECT
        ? AS topic,
        stories.story_id,
        stories.url,
        stories.domain,
        stories.publish_date,
        stories.media_name,
        stories.title,
        story_topic_state.qc_status,
        story_topic_state.qc_reason,
        story_topic_state.dup_of_story_id,
        articles.fetch_status,
        articles.source,
        articles.text_chars,
        latest_extractions.relevant,
        latest_extractions.reject_reason
    FROM reachable_stories
    JOIN stories ON stories.story_id = reachable_stories.story_id
    LEFT JOIN story_topic_state
        ON story_topic_state.topic = ?
       AND story_topic_state.story_id = stories.story_id
    LEFT JOIN articles ON articles.story_id = stories.story_id
    LEFT JOIN latest_extractions ON latest_extractions.story_id = stories.story_id
    ORDER BY stories.publish_date ASC, stories.story_id ASC
"""


def _reachable_story_count(connection: sqlite3.Connection, topic_name: str) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT search_hits.story_id
            FROM search_hits
            JOIN search_windows ON search_windows.id = search_hits.search_window_id
            WHERE search_windows.topic = ?
        )
        """,
        (topic_name,),
    ).fetchone()
    if row is None:
        raise ExportError("Failed to count reachable stories for export.")
    return int(row[0])


def _case_count(connection: sqlite3.Connection, topic_name: str, prompt_version: str) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*)
        FROM cases
        JOIN story_extractions ON story_extractions.id = cases.story_extraction_id
        WHERE story_extractions.topic = ?
          AND story_extractions.prompt_version = ?
          AND story_extractions.relevant = 1
        """,
        (topic_name, prompt_version),
    ).fetchone()
    if row is None:
        raise ExportError("Failed to count cases for export.")
    return int(row[0])


def export_topic(
    config: AppConfig,
    topic_name: str,
    connection: sqlite3.Connection,
    *,
    output_dir: Path = Path("data/out"),
) -> StageSummary:
    """Export one topic's stored cases and article coverage as RFC-4180 CSV files."""
    topic = _topic(config, topic_name)
    case_columns = case_csv_columns(topic)
    destination_dir = Path(output_dir)
    case_destination = destination_dir / f"{topic_name}.csv"
    audit_destination = destination_dir / f"{topic_name}_articles.csv"

    case_temporary: Path | None = None
    audit_temporary: Path | None = None
    try:
        destination_dir.mkdir(parents=True, exist_ok=True)
        with transaction(connection):
            expected_cases = _case_count(connection, topic_name, config.llm.prompt_version)
            expected_stories = _reachable_story_count(connection, topic_name)
            stored_case_columns = {
                str(row["name"]) for row in connection.execute("PRAGMA table_info(cases)")
            }
            case_rows = connection.execute(
                _case_query(topic, stored_case_columns),
                (topic_name, topic.link_category, topic_name, config.llm.prompt_version),
            )
            audit_rows = connection.execute(
                _ARTICLE_AUDIT_QUERY,
                (
                    topic_name,
                    topic_name,
                    config.llm.prompt_version,
                    topic_name,
                    topic_name,
                ),
            )
            case_temporary, written_cases = _write_csv(case_destination, case_columns, case_rows)
            audit_temporary, written_stories = _write_csv(
                audit_destination, ARTICLE_AUDIT_COLUMNS, audit_rows
            )

            if written_cases != expected_cases or written_stories != expected_stories:
                raise ExportError(
                    "Export row-count reconciliation failed: "
                    f"cases expected={expected_cases} written={written_cases}; "
                    f"stories expected={expected_stories} written={written_stories}."
                )

        if case_temporary is None or audit_temporary is None:
            raise ExportError("Export temporary files were not created.")
        os.replace(case_temporary, case_destination)
        case_temporary = None
        os.replace(audit_temporary, audit_destination)
        audit_temporary = None
    except (OSError, sqlite3.Error) as exc:
        raise ExportError(f"Failed to export topic {topic_name}: {exc}") from exc
    finally:
        if case_temporary is not None:
            case_temporary.unlink(missing_ok=True)
        if audit_temporary is not None:
            audit_temporary.unlink(missing_ok=True)

    return StageSummary(topic_name, expected_stories, expected_stories, 0, 0)
