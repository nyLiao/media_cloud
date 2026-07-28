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


def test_project_config_uses_simplified_transition_contract():
    config = load_config()
    extraction = config.topics["revolving_door_ca"].extraction

    assert [field.name for field in extraction.fields] == [
        "person_name",
        "cohort_name",
        "private_org",
        "private_time",
        "public_org",
        "public_time",
        "jurisdiction",
    ]
    assert extraction.exactly_one_of == ["person_name", "cohort_name"]
    assert {field.name for field in extraction.fields if field.required} == {
        "private_org",
        "public_org",
        "jurisdiction",
    }
    assert config.media_cloud.window_days == 5000
    assert config.media_cloud.max_retries == 0
    assert config.media_cloud.timeout_s == MIN_QUERY_TIMEOUT_S
    assert config.llm.timeout_s == MIN_QUERY_TIMEOUT_S


def _config_with(replacement: tuple[str, str], tmp_path: Path) -> Path:
    source = Path("config/topics.yaml").read_text(encoding="utf-8")
    old, new = replacement
    assert old in source, f"fixture anchor no longer present: {old!r}"
    config_path = tmp_path / "topics.yaml"
    config_path.write_text(source.replace(old, new, 1), encoding="utf-8")
    return config_path


def test_extraction_fields_must_be_the_fixed_seven_field_contract(tmp_path):
    path = _config_with(
        (
            "        - name: jurisdiction\n",
            "        - name: case_type\n"
            "          type: string\n"
            "          required: false\n"
            '          description: "Legacy discriminator."\n'
            "        - name: jurisdiction\n",
        ),
        tmp_path,
    )

    with pytest.raises(ConfigError, match="must be exactly"):
        load_config(path)


def test_person_and_cohort_must_be_the_exactly_one_fields(tmp_path):
    path = _config_with(
        (
            "      exactly_one_of: [person_name, cohort_name]",
            "      exactly_one_of: [person_name, private_org]",
        ),
        tmp_path,
    )

    with pytest.raises(ConfigError, match="exactly_one_of must be"):
        load_config(path)


def test_required_transition_fields_cannot_be_optional(tmp_path):
    path = _config_with(
        (
            "        - name: private_org\n          type: string\n          required: true",
            "        - name: private_org\n          type: string\n          required: false",
        ),
        tmp_path,
    )

    with pytest.raises(ConfigError, match="only private_org"):
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


@pytest.mark.parametrize("section", ["media_cloud", "llm"])
def test_query_timeouts_cannot_be_less_than_five_minutes(tmp_path, section):
    source = Path("config/topics.yaml").read_text(encoding="utf-8")
    section_start = source.index(f"{section}:")
    timeout_start = source.index("  timeout_s: 300", section_start)
    invalid = source[:timeout_start] + "  timeout_s: 299" + source[timeout_start + 16 :]
    config_path = tmp_path / f"{section}.yaml"
    config_path.write_text(invalid, encoding="utf-8")

    with pytest.raises(ConfigError, match="greater than or equal to 300"):
        load_config(config_path)
