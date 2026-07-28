"""Typed configuration and stage-specific credential loading."""

from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Literal

import yaml
from dotenv import dotenv_values
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    ValidationError,
    field_validator,
    model_validator,
)

from .errors import ConfigError, MissingCredentialError

MIN_QUERY_TIMEOUT_S = 300.0


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class MediaCloudConfig(StrictModel):
    api_key_env: str = "MC_API_TOKEN"
    platform: str = "onlinenews-mediacloud"
    window_days: int = Field(gt=0)
    page_size: int = Field(gt=0)
    max_pages_per_window: int = Field(gt=0)
    timeout_s: float = Field(ge=MIN_QUERY_TIMEOUT_S)
    max_retries: int = Field(ge=0)


class FetchConfig(StrictModel):
    user_agent: str
    per_domain_delay_s: float = Field(ge=1)
    global_max_rps: float = Field(gt=0)
    timeout_s: float = Field(gt=0)
    max_redirects: int = Field(gt=0)
    max_response_bytes: int = Field(gt=0)
    max_retries: int = Field(ge=0)
    respect_robots: bool
    wayback_fallback: bool
    min_text_chars: int = Field(gt=0)
    save_raw_html: bool


class LLMConfig(StrictModel):
    base_url_env: str = "LLM_BASE_URL"
    api_key_env: str = "LLM_API_KEY"
    model: str
    timeout_s: float = Field(ge=MIN_QUERY_TIMEOUT_S)
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
    exclude_title_terms: list[str] = Field(default_factory=list)

    @field_validator("exclude_title_terms")
    @classmethod
    def validate_exclude_title_terms(cls, terms: list[str]) -> list[str]:
        """Normalize unique title terms for deterministic complete-token matching."""
        normalized_terms = [
            " ".join("".join(char if char.isalnum() else " " for char in term.casefold()).split())
            for term in terms
        ]
        if any(not term for term in normalized_terms):
            raise ValueError("exclude_title_terms must contain at least one alphanumeric character")
        if len(set(normalized_terms)) != len(normalized_terms):
            raise ValueError("exclude_title_terms must not contain duplicates")
        return normalized_terms


class ExtractionField(StrictModel):
    name: str
    field_type: Literal["string"] = Field(alias="type")
    required: bool
    description: str


class ExtractionConfig(StrictModel):
    unit: str
    relevance_criteria: str
    fields: list[ExtractionField]
    exactly_one_of: list[str] = Field(min_length=2)

    @model_validator(mode="after")
    def validate_field_names(self) -> ExtractionConfig:
        names = [field.name for field in self.fields]
        duplicates = {name for name in names if names.count(name) > 1}
        if duplicates:
            raise ValueError(f"duplicate extraction field names: {sorted(duplicates)}")
        return self

    @model_validator(mode="after")
    def validate_transition_contract(self) -> ExtractionConfig:
        """Enforce the fixed, minimal transition-extraction contract."""
        by_name = {field.name: field for field in self.fields}
        expected_names = {
            "person_name",
            "cohort_name",
            "private_org",
            "private_time",
            "public_org",
            "public_time",
            "jurisdiction",
        }
        if set(by_name) != expected_names:
            raise ValueError(f"extraction.fields must be exactly: {sorted(expected_names)}")
        if self.exactly_one_of != ["person_name", "cohort_name"]:
            raise ValueError('exactly_one_of must be ["person_name", "cohort_name"]')
        if any(by_name[name].required for name in self.exactly_one_of):
            raise ValueError("exactly_one_of fields must allow null")

        required_names = {"private_org", "public_org", "jurisdiction"}
        if {name for name, field in by_name.items() if field.required} != required_names:
            raise ValueError("only private_org, public_org, and jurisdiction may be required")
        return self


class TopicConfig(StrictModel):
    label: str
    link_category: str
    query: str = Field(min_length=1)
    start_date: date
    end_date: date
    collection_ids: list[int] = Field(default_factory=list)
    source_ids: list[int] = Field(default_factory=list)
    languages: list[str]
    domain_allowlist: list[str]
    fetch_priority_title_terms: list[str] = Field(default_factory=list)
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


def _load_required_env(name: str, file_values: Mapping[str, str | None]) -> SecretStr:
    value = os.environ.get(name) or file_values.get(name)
    if not value:
        raise MissingCredentialError(f"Missing required environment variable: {name}")
    return SecretStr(value)


def load_media_cloud_token(config: AppConfig, env_file: str | Path = "config/.env") -> SecretStr:
    """Load the Media Cloud token only when a Media Cloud stage needs it."""
    file_values = dotenv_values(Path(env_file))
    return _load_required_env(config.media_cloud.api_key_env, file_values)


def load_llm_credentials(config: AppConfig, env_file: str | Path = "config/.env") -> LLMCredentials:
    """Load proxy credentials without altering the configured base URL."""
    file_values = dotenv_values(Path(env_file))
    base_url = _load_required_env(config.llm.base_url_env, file_values).get_secret_value()
    api_key = _load_required_env(config.llm.api_key_env, file_values)
    return LLMCredentials(base_url=base_url, api_key=api_key)
