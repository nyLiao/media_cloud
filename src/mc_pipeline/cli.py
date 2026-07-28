"""Command-line interface for the Media Cloud research pipeline."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Annotated

import typer

from .config import load_config, load_media_cloud_token
from .db import connect_db_readonly, init_db
from .errors import ConfigError, DatabaseError, MediaCloudError, MissingCredentialError

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


def _exit_code(error: BaseException) -> int:
    """Translate typed pipeline errors to the documented CLI exit codes."""
    if isinstance(error, MissingCredentialError):
        return 3
    if isinstance(error, ConfigError):
        return 2
    if isinstance(error, DatabaseError):
        return 4
    if isinstance(error, MediaCloudError):
        return 5
    raise error


def _run_command(operation: Callable[[], None]) -> None:
    """Run an operation and present known pipeline errors consistently."""
    try:
        operation()
    except (ConfigError, DatabaseError, MediaCloudError) as error:
        typer.echo(f"Error: {error}", err=True)
        raise typer.Exit(_exit_code(error)) from error


def _echo_summary(summary: object) -> None:
    """Print the service-provided StageSummary representation."""
    typer.echo(str(summary))


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


def main() -> None:
    """Run the Typer application."""
    app()


if __name__ == "__main__":
    main()
