"""Tests for CLI orchestration without live services or credentials."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import SecretStr
from typer.testing import CliRunner

from mc_pipeline import cli
from mc_pipeline.errors import ConfigError, DatabaseError, MediaCloudError, MissingCredentialError

runner = CliRunner()


@dataclass
class FakeConnection:
    closed: bool = False

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def stage_environment(monkeypatch):
    config = SimpleNamespace(media_cloud=SimpleNamespace(api_key_env="TEST_MC_TOKEN"))
    connections: list[FakeConnection] = []

    def fake_init_db(_: Path) -> FakeConnection:
        connection = FakeConnection()
        connections.append(connection)
        return connection

    monkeypatch.delenv("TEST_MC_TOKEN", raising=False)
    monkeypatch.setattr(cli, "load_config", lambda _: config)
    monkeypatch.setattr(cli, "load_media_cloud_token", lambda *_: SecretStr("test-token"))
    monkeypatch.setattr(cli, "_stage_connection", lambda _db, *, dry_run: fake_init_db(_db))
    monkeypatch.setattr(cli, "connect_db_readonly", fake_init_db)
    return config, connections


def test_estimate_loads_explicit_credentials_and_forwards_dry_run(stage_environment, monkeypatch):
    config, connections = stage_environment
    calls: list[tuple[object, str, FakeConnection, bool, str]] = []

    def estimate_topic(app_config, topic, connection, *, dry_run, api_token):
        calls.append((app_config, topic, connection, dry_run, api_token.get_secret_value()))
        return "estimated: 12 stories"

    monkeypatch.setattr(
        cli, "_search_module", lambda: SimpleNamespace(estimate_topic=estimate_topic)
    )

    result = runner.invoke(cli.app, ["estimate", "--topic", "sample", "--env-file", "test.env"])

    assert result.exit_code == 0
    assert result.stdout == "estimated: 12 stories\n"
    assert calls == [(config, "sample", connections[0], False, "test-token")]
    assert connections[0].closed


def test_search_forwards_window_limit_without_live_credentials(stage_environment, monkeypatch):
    config, connections = stage_environment
    calls: list[tuple[object, str, FakeConnection, int | None, bool]] = []

    def search_topic(app_config, topic, connection, *, limit_windows, dry_run, api_token):
        assert api_token is None
        calls.append((app_config, topic, connection, limit_windows, dry_run))
        return "search dry run"

    monkeypatch.setattr(cli, "_search_module", lambda: SimpleNamespace(search_topic=search_topic))

    result = runner.invoke(
        cli.app,
        ["search", "--topic", "sample", "--limit-windows", "2", "--dry-run"],
    )

    assert result.exit_code == 0
    assert result.stdout == "search dry run\n"
    assert calls == [(config, "sample", connections[0], 2, True)]
    assert connections[0].closed


def test_review_search_uses_topic_default_output(stage_environment, monkeypatch):
    config, connections = stage_environment
    calls: list[tuple[object, str, FakeConnection, Path]] = []

    def export_search_review(app_config, topic, connection, *, output):
        calls.append((app_config, topic, connection, output))
        return "reviewed: 4 windows"

    monkeypatch.setattr(
        cli, "_search_module", lambda: SimpleNamespace(export_search_review=export_search_review)
    )

    result = runner.invoke(cli.app, ["review-search", "--topic", "sample"])

    assert result.exit_code == 0
    assert result.stdout == (
        "reviewed: 4 windows\nSearch review CSV: data/review/sample-search.csv\n"
    )
    assert calls == [(config, "sample", connections[0], Path("data/review/sample-search.csv"))]
    assert connections[0].closed


def test_review_search_forwards_custom_output(stage_environment, monkeypatch):
    _, connections = stage_environment
    outputs: list[Path] = []

    def export_search_review(_, __, ___, *, output):
        outputs.append(output)
        return "reviewed"

    monkeypatch.setattr(
        cli, "_search_module", lambda: SimpleNamespace(export_search_review=export_search_review)
    )

    result = runner.invoke(
        cli.app,
        ["review-search", "--topic", "sample", "--output", "custom/review.csv"],
    )

    assert result.exit_code == 0
    assert outputs == [Path("custom/review.csv")]
    assert connections[0].closed


@pytest.mark.parametrize(
    ("error", "expected_code"),
    [
        (ConfigError("invalid configuration"), 2),
        (DatabaseError("database unavailable"), 4),
        (MediaCloudError("service unavailable"), 5),
    ],
)
def test_known_pipeline_errors_use_documented_exit_codes(error, expected_code, monkeypatch):
    monkeypatch.setattr(cli, "load_config", lambda _: (_ for _ in ()).throw(error))

    result = runner.invoke(cli.app, ["review-search", "--topic", "sample"])

    assert result.exit_code == expected_code
    assert f"Error: {error}" in result.stderr


def test_missing_credential_subtype_uses_exit_code_three(monkeypatch):
    monkeypatch.setattr(
        cli,
        "load_config",
        lambda _: (_ for _ in ()).throw(MissingCredentialError("missing")),
    )

    result = runner.invoke(cli.app, ["estimate", "--topic", "sample"])

    assert result.exit_code == 3
    assert "Error: missing" in result.stderr
