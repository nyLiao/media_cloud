"""Typed configuration and stage-specific credential loading."""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from typing import Literal

import yaml
from dotenv import dotenv_values
from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError, model_validator


class ConfigError(RuntimeError):
    """Raised when project configuration or required credentials are invalid."""


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class MediaCloudConfig(StrictModel):
    api_key_env: str = "MC_API_TOKEN"
    platform: str = "onlinenews-mediacloud"
    window_days: int = Field(gt=0)
    page_size: int = Field(gt=0)
    max_pages_per_window: int = Field(gt=0)


class FetchConfig(StrictModel):
    user_agent: str
    per_domain_delay_s: float = Field(ge=0)
    global_max_rps: float = Field(gt=0)
    timeout_s: float = Field(gt=0)
    max_retries: int = Field(ge=0)
    respect_robots: bool
    wayback_fallback: bool
    min_text_chars: int = Field(gt=0)
    save_raw_html: bool


class LLMConfig(StrictModel):
    base_url_env: str = "LLM_BASE_URL"
    api_key_env: str = "LLM_API_KEY"
    model: str
    max_requests_per_min: int = Field(gt=0)
    articles_per_request: int = Field(gt=0)
    max_article_chars: int = Field(gt=0)
    max_output_tokens: int = Field(gt=0)
    temperature: float
    structured_output: Literal["json_schema", "json_object"]
    prompt_version: str


class QCConfig(StrictModel):
    min_title_chars: int = Field(ge=0)
    exclude_url_patterns: list[str]
    near_dup_threshold: int = Field(ge=0, le=100)


class ExtractionField(StrictModel):
    name: str
    field_type: Literal["string", "integer", "enum"] = Field(alias="type")
    required: bool
    description: str
    values: list[str] | None = None

    @model_validator(mode="after")
    def validate_enum_values(self) -> ExtractionField:
        if self.field_type == "enum" and not self.values:
            raise ValueError("enum extraction fields require values")
        if self.field_type != "enum" and self.values is not None:
            raise ValueError("only enum extraction fields may define values")
        return self


class ExtractionConfig(StrictModel):
    unit: str
    relevance_criteria: str
    fields: list[ExtractionField]


class TopicConfig(StrictModel):
    label: str
    query: str = Field(min_length=1)
    start_date: date
    end_date: date
    collection_ids: list[int] = Field(default_factory=list)
    source_ids: list[int] = Field(default_factory=list)
    languages: list[str]
    domain_allowlist: list[str]
    qc: QCConfig
    extraction: ExtractionConfig

    @model_validator(mode="after")
    def validate_search_scope(self) -> TopicConfig:
        if self.end_date < self.start_date:
            raise ValueError("end_date must be on or after start_date")
        if not self.collection_ids and not self.source_ids:
            raise ValueError("at least one collection_id or source_id is required")
        return self


class AppConfig(StrictModel):
    media_cloud: MediaCloudConfig
    fetch: FetchConfig
    llm: LLMConfig
    topics: dict[str, TopicConfig]


class LLMCredentials(StrictModel):
    base_url: str
    api_key: SecretStr


def load_config(path: str | Path = "config/topics.yaml") -> AppConfig:
    """Parse and validate the non-secret YAML configuration."""
    config_path = Path(path)
    try:
        payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        return AppConfig.model_validate(payload)
    except (OSError, yaml.YAMLError, ValidationError) as exc:
        raise ConfigError(f"Invalid configuration at {config_path}: {exc}") from exc


def _load_required_env(name: str, env_file: str | Path) -> SecretStr:
    file_values = dotenv_values(Path(env_file))
    value = os.environ.get(name) or file_values.get(name)
    if not value:
        raise ConfigError(f"Missing required environment variable: {name}")
    return SecretStr(value)


def load_media_cloud_token(config: AppConfig, env_file: str | Path = "config/.env") -> SecretStr:
    """Load the Media Cloud token only when a Media Cloud stage needs it."""
    return _load_required_env(config.media_cloud.api_key_env, env_file)


def load_llm_credentials(config: AppConfig, env_file: str | Path = "config/.env") -> LLMCredentials:
    """Load proxy credentials without altering the configured base URL."""
    base_url = _load_required_env(config.llm.base_url_env, env_file).get_secret_value()
    api_key = _load_required_env(config.llm.api_key_env, env_file)
    return LLMCredentials(base_url=base_url, api_key=api_key)
