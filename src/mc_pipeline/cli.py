"""Command-line interface for the Media Cloud research pipeline."""

from __future__ import annotations

import sqlite3
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Annotated, Literal

import typer
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from .config import load_config, load_llm_credentials, load_media_cloud_token
from .db import connect_db_readonly, init_db
from .errors import (
    ConfigError,
    DatabaseError,
    ExportError,
    ExtractionError,
    FetchError,
    MediaCloudError,
    MissingCredentialError,
)
from .identity import canonical_json
from .stage import StageSummary

if TYPE_CHECKING:
    from .extract import ExtractionProgress
    from .fetch import FetchProgress

app = typer.Typer(help="Media Cloud research pipeline.", no_args_is_help=True)

DEFAULT_CONFIG_PATH = Path("config/topics.yaml")
DEFAULT_ENV_FILE = Path("config/.env")
DEFAULT_DATABASE_PATH = Path("data/mc.db")


def _stage_connection(db: Path, *, dry_run: bool) -> sqlite3.Connection:
    if not dry_run:
        return init_db(db)
    if db.is_file():
        return connect_db_readonly(db)
    return init_db(":memory:")


def _search_module() -> ModuleType:
    """Load the Stage 1 service only when a Stage 1 command runs."""
    from . import search

    return search


def _dedup_module() -> ModuleType:
    """Load the Stage 2 service only when the Stage 2 command runs."""
    from . import dedup

    return dedup


def _fetch_module() -> ModuleType:
    """Load the Stage 3 service only when the Stage 3 command runs."""
    from . import fetch

    return fetch


def _extract_module() -> ModuleType:
    """Load Stage 4 only when an extraction command runs."""
    from . import extract

    return extract


def _export_module() -> ModuleType:
    """Load Stage 5 only when an export command runs."""
    from . import export

    return export


class _RichFetchProgressReporter:
    """Render bounded Stage 3 progress without exposing titles or URLs."""

    def __init__(self, *, enabled: bool) -> None:
        self._console = Console(stderr=True)
        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            MofNCompleteColumn(),
            TextColumn("ok={task.fields[succeeded]} failed={task.fields[failed]}"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=self._console,
            disable=not enabled,
        )
        self._task_id: TaskID | None = None

    def __call__(self, update: FetchProgress) -> None:
        processed = update.processed
        total = update.total
        domain = " ".join(update.domain.split())[:60]
        status = " ".join(update.status.split())[:24]
        succeeded = update.succeeded
        failed = update.failed
        self._console.print(
            canonical_json(
                {
                    "event": "fetch_entry",
                    "processed": processed,
                    "total": total,
                    "story_id": update.story_id,
                    "domain": domain,
                    "status": status,
                    "succeeded": succeeded,
                    "failed": failed,
                }
            ),
            markup=False,
            highlight=False,
            soft_wrap=True,
        )
        if self._task_id is None:
            self._progress.start()
            self._task_id = self._progress.add_task(
                "starting",
                total=total,
                succeeded=0,
                failed=0,
            )
        self._progress.update(
            self._task_id,
            completed=processed,
            description=f"{domain} [{status}]",
            succeeded=succeeded,
            failed=failed,
        )

    def close(self) -> None:
        if self._task_id is not None:
            self._progress.stop()


class _RichExtractionProgressReporter:
    """Render bounded live Stage 4 batch progress."""

    def __init__(self, *, enabled: bool) -> None:
        self._console = Console(stderr=True, force_terminal=enabled)
        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            MofNCompleteColumn(),
            TextColumn("ok={task.fields[succeeded]} failed={task.fields[failed]}"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=self._console,
            disable=not enabled,
        )
        self._task_id: TaskID | None = None

    def __call__(self, update: ExtractionProgress) -> None:
        if self._task_id is None:
            self._progress.start()
            self._task_id = self._progress.add_task(
                "starting",
                total=update.total,
                succeeded=0,
                failed=0,
            )
        self._progress.update(
            self._task_id,
            completed=update.processed,
            description=f"batch {update.batch}/{update.batch_count} [{update.status}]",
            succeeded=update.succeeded,
            failed=update.failed,
        )

    def event(self, detail: str) -> None:
        """Print one structured extraction event without disrupting live progress."""
        self._console.print(detail, markup=False, highlight=False, soft_wrap=True)
        self._console.file.flush()

    def close(self) -> None:
        if self._task_id is not None:
            self._progress.stop()


def _exit_code(error: BaseException) -> int:
    """Translate typed pipeline errors to the documented CLI exit codes."""
    if isinstance(error, MissingCredentialError):
        return 3
    if isinstance(error, ConfigError):
        return 2
    if isinstance(error, DatabaseError):
        return 4
    if isinstance(error, (MediaCloudError, FetchError)):
        return 5
    if isinstance(error, ExtractionError):
        return 6
    if isinstance(error, ExportError):
        return 7
    raise error


def _run_command(operation: Callable[[], None]) -> None:
    """Run an operation and present known pipeline errors consistently."""
    try:
        operation()
    except (ConfigError, DatabaseError, MediaCloudError, FetchError) as error:
        typer.echo(f"Error: {error}", err=True)
        raise typer.Exit(_exit_code(error)) from error


def _echo_summary(summary: object) -> None:
    """Print the service-provided StageSummary representation."""
    typer.echo(str(summary))


def _echo_extract_summary(summary: StageSummary) -> None:
    """Print only final extraction counts after live request event output."""
    summary_line = (
        f"topic={summary.topic} processed={summary.processed} "
        f"succeeded={summary.succeeded} skipped={summary.skipped} failed={summary.failed}"
    )
    typer.echo(summary_line)


@app.callback()
def root() -> None:
    """Run the Media Cloud research pipeline."""


@app.command("init-db")
def init_database(
    db: Annotated[Path, typer.Option("--db", help="SQLite database path.")] = DEFAULT_DATABASE_PATH,
) -> None:
    """Create or migrate the pipeline SQLite database."""

    def operation() -> None:
        connection = init_db(db)
        try:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
        finally:
            connection.close()
        typer.echo(f"Initialized database at {db} (schema version {version}).")

    _run_command(operation)


@app.command("estimate")
def estimate(
    topic: Annotated[str, typer.Option("--topic", help="Configured topic name.")],
    config_path: Annotated[
        Path, typer.Option("--config", help="YAML configuration path.")
    ] = DEFAULT_CONFIG_PATH,
    env_file: Annotated[
        Path, typer.Option("--env-file", help="Media Cloud credentials file.")
    ] = DEFAULT_ENV_FILE,
    db: Annotated[Path, typer.Option("--db", help="SQLite database path.")] = DEFAULT_DATABASE_PATH,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Print the estimate request without calling Media Cloud."),
    ] = False,
) -> None:
    """Estimate matching stories without fetching result pages."""

    def operation() -> None:
        app_config = load_config(config_path)
        api_token = None if dry_run else load_media_cloud_token(app_config, env_file)
        connection = _stage_connection(db, dry_run=dry_run)
        try:
            summary = _search_module().estimate_topic(
                app_config, topic, connection, dry_run=dry_run, api_token=api_token
            )
        finally:
            connection.close()
        _echo_summary(summary)

    _run_command(operation)


@app.command("search")
def search(
    topic: Annotated[str, typer.Option("--topic", help="Configured topic name.")],
    config_path: Annotated[
        Path, typer.Option("--config", help="YAML configuration path.")
    ] = DEFAULT_CONFIG_PATH,
    env_file: Annotated[
        Path, typer.Option("--env-file", help="Media Cloud credentials file.")
    ] = DEFAULT_ENV_FILE,
    db: Annotated[Path, typer.Option("--db", help="SQLite database path.")] = DEFAULT_DATABASE_PATH,
    limit_windows: Annotated[
        int | None,
        typer.Option("--limit-windows", min=1, help="Process at most this many date windows."),
    ] = None,
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Print search requests without calling Media Cloud.")
    ] = False,
) -> None:
    """Search Media Cloud and persist result pages by date window."""

    def operation() -> None:
        app_config = load_config(config_path)
        api_token = None if dry_run else load_media_cloud_token(app_config, env_file)
        connection = _stage_connection(db, dry_run=dry_run)
        try:
            summary = _search_module().search_topic(
                app_config,
                topic,
                connection,
                limit_windows=limit_windows,
                dry_run=dry_run,
                api_token=api_token,
            )
        finally:
            connection.close()
        _echo_summary(summary)

    _run_command(operation)


@app.command("dedup")
def deduplicate(
    topic: Annotated[str, typer.Option("--topic", help="Configured topic name.")],
    config_path: Annotated[
        Path, typer.Option("--config", help="YAML configuration path.")
    ] = DEFAULT_CONFIG_PATH,
    db: Annotated[Path, typer.Option("--db", help="SQLite database path.")] = DEFAULT_DATABASE_PATH,
) -> None:
    """Deduplicate and quality-check persisted topic stories."""

    def operation() -> None:
        app_config = load_config(config_path)
        connection = init_db(db)
        try:
            summary = _dedup_module().deduplicate_topic(app_config, topic, connection)
        finally:
            connection.close()
        _echo_summary(summary)

    _run_command(operation)


@app.command("fetch")
def fetch_articles(
    topic: Annotated[str, typer.Option("--topic", help="Configured topic name.")],
    config_path: Annotated[
        Path, typer.Option("--config", help="YAML configuration path.")
    ] = DEFAULT_CONFIG_PATH,
    db: Annotated[Path, typer.Option("--db", help="SQLite database path.")] = DEFAULT_DATABASE_PATH,
    limit: Annotated[
        int | None,
        typer.Option("--limit", min=1, help="Process at most this many candidate stories."),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Preview candidates without network or database writes."),
    ] = False,
    progress: Annotated[
        bool | None,
        typer.Option(
            "--progress/--no-progress",
            help="Show or suppress interactive fetch progress (default: auto).",
        ),
    ] = None,
) -> None:
    """Acquire and cache full text for QC-passing stories."""

    def operation() -> None:
        app_config = load_config(config_path)
        connection = _stage_connection(db, dry_run=dry_run)
        progress_enabled = False
        if not dry_run:
            progress_enabled = sys.stderr.isatty() if progress is None else progress
        reporter = _RichFetchProgressReporter(enabled=progress_enabled)
        try:
            summary = _fetch_module().fetch_topic(
                app_config,
                topic,
                connection,
                limit=limit,
                dry_run=dry_run,
                progress=reporter,
            )
        finally:
            reporter.close()
            connection.close()
        _echo_summary(summary)

    _run_command(operation)


@app.command("extract")
def extract_cases(
    topic: Annotated[str, typer.Option("--topic", help="Configured topic name.")],
    config_path: Annotated[
        Path, typer.Option("--config", help="YAML configuration path.")
    ] = DEFAULT_CONFIG_PATH,
    env_file: Annotated[
        Path, typer.Option("--env-file", help="LLM credentials file.")
    ] = DEFAULT_ENV_FILE,
    db: Annotated[Path, typer.Option("--db", help="SQLite database path.")] = DEFAULT_DATABASE_PATH,
    limit: Annotated[
        int | None,
        typer.Option("--limit", min=1, help="Process at most this many candidate articles."),
    ] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="Directory for live Stage 5 CSV snapshots.")
    ] = Path("data/out"),
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Preview token-bounded batches without writes or network."),
    ] = False,
    progress: Annotated[
        bool | None,
        typer.Option(
            "--progress/--no-progress",
            help="Show or suppress interactive extraction progress (default: auto).",
        ),
    ] = None,
) -> None:
    """Extract structured cases from stored article text."""

    def operation() -> None:
        app_config = load_config(config_path)
        credentials = None if dry_run else load_llm_credentials(app_config, env_file)
        connection = _stage_connection(db, dry_run=dry_run)
        progress_enabled = False
        if not dry_run:
            progress_enabled = sys.stderr.isatty() if progress is None else progress
        reporter = _RichExtractionProgressReporter(enabled=progress_enabled)
        try:
            summary = _extract_module().extract_topic(
                app_config,
                topic,
                connection,
                limit=limit,
                dry_run=dry_run,
                credentials=credentials,
                output_dir=output_dir,
                progress=reporter,
                detail_reporter=reporter.event,
            )
        finally:
            reporter.close()
            connection.close()
        _echo_extract_summary(summary)

    _run_command(operation)


@app.command("extract-prompt")
def extract_prompt(
    topic: Annotated[str, typer.Option("--topic", help="Configured topic name.")],
    limit: Annotated[
        int | None,
        typer.Option("--limit", min=1, help="Bound candidate articles before prompt packing."),
    ] = None,
    batch: Annotated[
        int,
        typer.Option("--batch", min=1, help="One-based packed batch to write."),
    ] = 1,
    extraction_batch_id: Annotated[
        int | None,
        typer.Option("--batch-id", min=1, help="Reconstruct an exact stored extraction batch."),
    ] = None,
    phase: Annotated[
        Literal["screening", "extraction"],
        typer.Option("--phase", help="Planned prompt phase: screening or extraction."),
    ] = "screening",
    config_path: Annotated[
        Path, typer.Option("--config", help="YAML configuration path.")
    ] = DEFAULT_CONFIG_PATH,
    db: Annotated[Path, typer.Option("--db", help="SQLite database path.")] = DEFAULT_DATABASE_PATH,
    output: Annotated[
        Path | None, typer.Option("--output", help="Secret-free manual prompt text path.")
    ] = None,
) -> None:
    """Write one exact no-network LLM prompt pair for manual verification."""

    def operation() -> None:
        app_config = load_config(config_path)
        output_path = output or Path("data/review") / f"{topic}-llm-prompts.txt"
        connection = connect_db_readonly(db)
        try:
            artifact = _extract_module().build_manual_request(
                app_config,
                topic,
                connection,
                limit=limit,
                batch_number=batch,
                extraction_batch_id=extraction_batch_id,
                phase=phase,
            )
        finally:
            connection.close()
        _extract_module().write_manual_request(artifact, output_path)
        source = (
            f"stored_batch={artifact['source_extraction_batch_id']}"
            if "source_extraction_batch_id" in artifact
            else f"batch={artifact['batch_number']}/{artifact['batch_count']}"
        )
        typer.echo(
            f"Manual LLM prompts: {output_path} ({source}, "
            f"phase={artifact['phase']}, articles={len(artifact['story_ids'])}, "
            f"estimated_input_tokens={artifact['estimated_input_tokens']})"
        )

    _run_command(operation)


@app.command("export")
def export_results(
    topic: Annotated[str, typer.Option("--topic", help="Configured topic name.")],
    config_path: Annotated[
        Path, typer.Option("--config", help="YAML configuration path.")
    ] = DEFAULT_CONFIG_PATH,
    db: Annotated[Path, typer.Option("--db", help="SQLite database path.")] = DEFAULT_DATABASE_PATH,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="Directory for Stage 5 CSV files.")
    ] = Path("data/out"),
) -> None:
    """Export case and article-audit CSV snapshots."""

    def operation() -> None:
        app_config = load_config(config_path)
        connection = connect_db_readonly(db)
        try:
            summary = _export_module().export_topic(
                app_config, topic, connection, output_dir=output_dir
            )
        finally:
            connection.close()
        _echo_summary(summary)

    _run_command(operation)


@app.command("review-search")
def review_search(
    topic: Annotated[str, typer.Option("--topic", help="Configured topic name.")],
    config_path: Annotated[
        Path, typer.Option("--config", help="YAML configuration path.")
    ] = DEFAULT_CONFIG_PATH,
    db: Annotated[Path, typer.Option("--db", help="SQLite database path.")] = DEFAULT_DATABASE_PATH,
    output: Annotated[Path | None, typer.Option("--output", help="Search-review CSV path.")] = None,
) -> None:
    """Export persisted Stage 1 results for manual review."""

    def operation() -> None:
        app_config = load_config(config_path)
        review_output = output or Path("data/review") / f"{topic}-search.csv"
        connection = connect_db_readonly(db)
        try:
            summary = _search_module().export_search_review(
                app_config, topic, connection, output=review_output
            )
        finally:
            connection.close()
        _echo_summary(summary)
        typer.echo(f"Search review CSV: {review_output}")

    _run_command(operation)


@app.command("review-fetch")
def review_fetch(
    topic: Annotated[str, typer.Option("--topic", help="Configured topic name.")],
    limit: Annotated[
        int,
        typer.Option(
            "--limit",
            min=1,
            max=100,
            help="Maximum successful articles to export (required; 1-100).",
        ),
    ],
    config_path: Annotated[
        Path, typer.Option("--config", help="YAML configuration path.")
    ] = DEFAULT_CONFIG_PATH,
    db: Annotated[Path, typer.Option("--db", help="SQLite database path.")] = DEFAULT_DATABASE_PATH,
    output: Annotated[Path | None, typer.Option("--output", help="Fetch-review CSV path.")] = None,
) -> None:
    """Export a bounded sample of successfully fetched article text."""

    def operation() -> None:
        app_config = load_config(config_path)
        review_output = output or Path("data/review") / f"{topic}-fetch.csv"
        connection = connect_db_readonly(db)
        try:
            summary = _fetch_module().export_fetch_review(
                app_config,
                topic,
                connection,
                limit=limit,
                output=review_output,
            )
        finally:
            connection.close()
        _echo_summary(summary)
        typer.echo(f"Fetch review CSV: {review_output}")

    _run_command(operation)


@app.command("review-dedup")
def review_dedup(
    topic: Annotated[str, typer.Option("--topic", help="Configured topic name.")],
    config_path: Annotated[
        Path, typer.Option("--config", help="YAML configuration path.")
    ] = DEFAULT_CONFIG_PATH,
    db: Annotated[Path, typer.Option("--db", help="SQLite database path.")] = DEFAULT_DATABASE_PATH,
    output: Annotated[Path | None, typer.Option("--output", help="Dedup-review CSV path.")] = None,
) -> None:
    """Export persisted Stage 2 QC and deduplication outcomes for review."""

    def operation() -> None:
        app_config = load_config(config_path)
        review_output = output or Path("data/review") / f"{topic}-dedup.csv"
        connection = connect_db_readonly(db)
        try:
            summary = _dedup_module().export_dedup_review(
                app_config, topic, connection, output=review_output
            )
        finally:
            connection.close()
        _echo_summary(summary)
        typer.echo(f"Dedup review CSV: {review_output}")

    _run_command(operation)


def main() -> None:
    """Run the Typer application."""
    app()


if __name__ == "__main__":
    main()
