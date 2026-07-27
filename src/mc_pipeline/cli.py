"""Command-line interface for the Media Cloud research pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from .db import init_db

app = typer.Typer(help="Media Cloud research pipeline.", no_args_is_help=True)


@app.callback()
def root() -> None:
    """Run the Media Cloud research pipeline."""


@app.command("init-db")
def init_database(
    db: Annotated[Path, typer.Option("--db", help="SQLite database path.")] = Path("data/mc.db"),
) -> None:
    """Create or migrate the pipeline SQLite database."""
    connection = init_db(db)
    try:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
    finally:
        connection.close()
    typer.echo(f"Initialized database at {db} (schema version {version}).")


def main() -> None:
    """Run the Typer application."""
    app()


if __name__ == "__main__":
    main()
