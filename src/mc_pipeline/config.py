"""Typed configuration and stage-specific credential loading."""

from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Literal

import yaml
from dotenv import dotenv_values
from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError, model_validator

from .errors import ConfigError

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
    discriminator: str | None = None
    required_by_discriminator: dict[str, list[str]] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_field_names(self) -> ExtractionConfig:
        names = [field.name for field in self.fields]
        duplicates = {name for name in names if names.count(name) > 1}
        if duplicates:
            raise ValueError(f"duplicate extraction field names: {sorted(duplicates)}")
        return self

    @model_validator(mode="after")
    def validate_conditional_requirements(self) -> ExtractionConfig:
        """Check the conditional-requirement rules against the declared fields.

        ``required`` on a field means "never null". Requirements that hold only for
        some variants (a cohort finding has no person name) belong here instead,
        because a strict JSON Schema cannot express them.
        """
        by_name = {field.name: field for field in self.fields}

        if self.discriminator is None:
            if self.required_by_discriminator:
                raise ValueError("required_by_discriminator needs a discriminator field")
            return self

        discriminator_field = by_name.get(self.discriminator)
        if discriminator_field is None:
            raise ValueError(f"discriminator {self.discriminator!r} is not a declared field")
        if discriminator_field.field_type != "enum":
            raise ValueError(f"discriminator {self.discriminator!r} must be an enum field")
        if not discriminator_field.required:
            raise ValueError(f"discriminator {self.discriminator!r} must be required")

        allowed_variants = set(discriminator_field.values or ())
        for variant, required_names in self.required_by_discriminator.items():
            if variant not in allowed_variants:
                raise ValueError(
                    f"required_by_discriminator variant {variant!r} is not a value of "
                    f"{self.discriminator!r}: expected one of {sorted(allowed_variants)}"
                )
            for name in required_names:
                if name not in by_name:
                    raise ValueError(
                        f"required_by_discriminator[{variant!r}] names unknown field {name!r}"
                    )
                if name == self.discriminator:
                    raise ValueError("the discriminator cannot require itself")
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
        raise ConfigError(f"Missing required environment variable: {name}")
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
