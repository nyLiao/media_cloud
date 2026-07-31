#!/usr/bin/env python3
"""Back up and rerun all stored LLM analysis for one topic.

The reset is deliberately opt-in. It removes only per-story screening/extraction
state for the selected topic, allowing ``mc-pipeline extract`` to perform both
phases again. Historical request batches remain in the primary database and a
complete SQLite backup is made before any reset.
"""

from __future__ import annotations

import argparse
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", required=True, help="Configured topic to reset and rerun.")
    parser.add_argument("--db", type=Path, default=Path("data/mc.db"), help="SQLite database path.")
    parser.add_argument(
        "--config", type=Path, default=Path("config/topics.yaml"), help="YAML configuration path."
    )
    parser.add_argument(
        "--env-file", type=Path, default=Path("config/.env"), help="LLM credentials file."
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("data/out"), help="Live CSV snapshot directory."
    )
    parser.add_argument(
        "--backup-dir",
        type=Path,
        default=Path("data/backups"),
        help="Directory for the pre-reset SQLite backup.",
    )
    parser.add_argument(
        "--apply", action="store_true", help="Create the backup, reset the topic, and call extract."
    )
    parser.add_argument("--progress", action="store_true", help="Show extraction progress.")
    parser.add_argument("--no-progress", action="store_true", help="Suppress extraction progress.")
    return parser


def _analysis_counts(connection: sqlite3.Connection, topic: str) -> tuple[int, int]:
    screenings = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM story_extractions AS se
            JOIN extraction_batches AS eb ON eb.id = se.extraction_batch_id
            WHERE se.topic = ? AND json_extract(eb.request_json, '$.phase') = 'screening'
            """,
            (topic,),
        ).fetchone()[0]
    )
    extractions = int(
        connection.execute(
            "SELECT COUNT(*) FROM story_extractions WHERE topic = ?", (topic,)
        ).fetchone()[0]
    )
    return screenings, extractions - screenings


def _backup_database(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        raise FileExistsError(f"Backup already exists: {destination}")
    with (
        sqlite3.connect(source) as source_connection,
        sqlite3.connect(destination) as backup_connection,
    ):
        source_connection.backup(backup_connection)


def _reset_topic_analysis(connection: sqlite3.Connection, topic: str) -> None:
    connection.execute("PRAGMA foreign_keys = ON")
    with connection:
        connection.execute(
            """
            DELETE FROM cases
            WHERE story_extraction_id IN (
                SELECT id FROM story_extractions WHERE topic = ?
            )
            """,
            (topic,),
        )
        connection.execute("DELETE FROM story_extractions WHERE topic = ?", (topic,))


def _extract_command(args: argparse.Namespace, *, dry_run: bool) -> list[str]:
    command = [
        "uv",
        "run",
        "mc-pipeline",
        "extract",
        "--topic",
        args.topic,
        "--config",
        str(args.config),
        "--env-file",
        str(args.env_file),
        "--db",
        str(args.db),
        "--output-dir",
        str(args.output_dir),
    ]
    if dry_run:
        command.append("--dry-run")
    elif args.no_progress:
        command.append("--no-progress")
    elif args.progress:
        command.append("--progress")
    return command


def main() -> int:
    args = _parser().parse_args()
    if args.progress and args.no_progress:
        raise SystemExit("Choose only one of --progress or --no-progress.")
    if not args.db.is_file():
        raise SystemExit(f"Database does not exist: {args.db}")

    with sqlite3.connect(args.db) as connection:
        screenings, extractions = _analysis_counts(connection, args.topic)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")  # noqa: UP017
    backup_path = args.backup_dir / f"{args.db.stem}-{args.topic}-llm-before-rerun-{timestamp}.db"
    print(
        f"Topic {args.topic!r}: {screenings} screenings and {extractions} extractions "
        "will be reset."
    )
    print(f"Pre-reset backup: {backup_path}")
    if not args.apply:
        print("Dry run only. Re-run with --apply to back up, reset, and rerun both LLM phases.")
        return 0

    preflight = subprocess.run(_extract_command(args, dry_run=True), check=False)
    if preflight.returncode != 0:
        return preflight.returncode
    _backup_database(args.db, backup_path)
    with sqlite3.connect(args.db) as connection:
        _reset_topic_analysis(connection, args.topic)
    print("Reset complete; historical batch provenance remains in the database and backup.")
    return subprocess.run(_extract_command(args, dry_run=False), check=False).returncode


if __name__ == "__main__":
    sys.exit(main())
