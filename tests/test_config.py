from __future__ import annotations

from pathlib import Path

import pytest

from mc_pipeline.config import (
    ConfigError,
    load_config,
    load_llm_credentials,
    load_media_cloud_token,
)


def test_project_config_parses_with_verified_scope():
    config = load_config()
    topic = config.topics["revolving_door_ca"]

    assert topic.start_date.isoformat() == "2021-01-01"
    assert topic.end_date.isoformat() == "2025-12-31"
    assert topic.collection_ids == [34411583]
    assert '"revolving door"' in topic.query
    assert topic.domain_allowlist == []


def test_stage_credentials_load_from_explicit_env_file(tmp_path, monkeypatch):
    monkeypatch.delenv("MC_API_TOKEN", raising=False)
    monkeypatch.delenv("LLM_BASE_URL", raising=False)
    monkeypatch.delenv("LLM_API_KEY", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text(
        "MC_API_TOKEN=mc-secret\nLLM_BASE_URL=https://proxy.example/v1\nLLM_API_KEY=llm-secret\n",
        encoding="utf-8",
    )
    config = load_config()

    token = load_media_cloud_token(config, env_file)
    credentials = load_llm_credentials(config, env_file)

    assert token.get_secret_value() == "mc-secret"
    assert credentials.base_url == "https://proxy.example/v1"
    assert credentials.api_key.get_secret_value() == "llm-secret"
    assert "mc-secret" not in repr(token)
    assert "llm-secret" not in repr(credentials)


def test_missing_stage_secret_fails_without_loading_other_stage(tmp_path, monkeypatch):
    monkeypatch.delenv("MC_API_TOKEN", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("LLM_BASE_URL=https://proxy.example/v1\n", encoding="utf-8")
    config = load_config()

    with pytest.raises(ConfigError, match="MC_API_TOKEN"):
        load_media_cloud_token(config, env_file)


def test_invalid_topic_scope_is_rejected(tmp_path):
    source = Path("config/topics.yaml").read_text(encoding="utf-8")
    invalid = source.replace("collection_ids: [34411583]", "collection_ids: []")
    config_path = tmp_path / "topics.yaml"
    config_path.write_text(invalid, encoding="utf-8")

    with pytest.raises(ConfigError, match="collection_id or source_id"):
        load_config(config_path)
