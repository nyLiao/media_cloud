from __future__ import annotations

from pathlib import Path

import pytest

from mc_pipeline.config import (
    MIN_QUERY_TIMEOUT_S,
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
    assert topic.link_category == "Revolving Door"
    assert topic.extraction.discriminator == "case_type"
    assert topic.extraction.required_by_discriminator == {
        "individual": ["person_name", "previous_org", "current_org"],
        "cohort": ["cohort_name"],
    }
    assert config.media_cloud.timeout_s == MIN_QUERY_TIMEOUT_S
    assert config.llm.timeout_s == MIN_QUERY_TIMEOUT_S


def _config_with(replacement: tuple[str, str], tmp_path: Path) -> Path:
    source = Path("config/topics.yaml").read_text(encoding="utf-8")
    old, new = replacement
    assert old in source, f"fixture anchor no longer present: {old!r}"
    config_path = tmp_path / "topics.yaml"
    config_path.write_text(source.replace(old, new, 1), encoding="utf-8")
    return config_path


def test_unknown_discriminator_variant_is_rejected(tmp_path):
    path = _config_with(("        cohort: [cohort_name]", "        group: [cohort_name]"), tmp_path)
    with pytest.raises(ConfigError, match="is not a value of"):
        load_config(path)


def test_conditional_rule_naming_unknown_field_is_rejected(tmp_path):
    path = _config_with(
        ("        cohort: [cohort_name]", "        cohort: [cohort_nickname]"), tmp_path
    )
    with pytest.raises(ConfigError, match="unknown field"):
        load_config(path)


def test_discriminator_must_be_a_declared_enum_field(tmp_path):
    path = _config_with(("      discriminator: case_type", "      discriminator: claim"), tmp_path)
    with pytest.raises(ConfigError, match="must be an enum field"):
        load_config(path)


def test_conditional_rules_require_a_discriminator(tmp_path):
    path = _config_with(("      discriminator: case_type\n", ""), tmp_path)
    with pytest.raises(ConfigError, match="needs a discriminator"):
        load_config(path)


def test_duplicate_extraction_field_names_are_rejected(tmp_path):
    path = _config_with(
        (
            "        - name: cohort_period",
            "        - name: person_name\n          type: string\n"
            '          required: false\n          description: "Duplicate."\n'
            "        - name: cohort_period",
        ),
        tmp_path,
    )
    with pytest.raises(ConfigError, match="duplicate extraction field names"):
        load_config(path)


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


def test_llm_credentials_parse_env_file_once(tmp_path, monkeypatch):
    monkeypatch.delenv("LLM_BASE_URL", raising=False)
    monkeypatch.delenv("LLM_API_KEY", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text(
        "LLM_BASE_URL=https://proxy.example/v1\nLLM_API_KEY=llm-secret\n",
        encoding="utf-8",
    )
    config = load_config()
    calls = 0

    from mc_pipeline import config as config_module

    real_dotenv_values = config_module.dotenv_values

    def counting_dotenv_values(path):
        nonlocal calls
        calls += 1
        return real_dotenv_values(path)

    monkeypatch.setattr(config_module, "dotenv_values", counting_dotenv_values)

    load_llm_credentials(config, env_file)

    assert calls == 1


@pytest.mark.parametrize("section", ["media_cloud", "llm"])
def test_query_timeouts_cannot_be_less_than_five_minutes(tmp_path, section):
    anchor = "  timeout_s: 300"
    source = Path("config/topics.yaml").read_text(encoding="utf-8")
    section_start = source.index(f"{section}:")
    timeout_start = source.index(anchor, section_start)
    invalid = source[:timeout_start] + "  timeout_s: 299" + source[timeout_start + len(anchor) :]
    config_path = tmp_path / f"{section}.yaml"
    config_path.write_text(invalid, encoding="utf-8")

    with pytest.raises(ConfigError, match="greater than or equal to 300"):
        load_config(config_path)


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
