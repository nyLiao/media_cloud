"""Shapes derived from topic extraction configuration.

``extraction.fields`` in ``config/topics.yaml`` is the single declaration of what the
LLM must return. Every downstream shape is derived from it here -- the JSON Schema
sent to the provider, the model that validates the response, and the CSV column
order -- so the three cannot drift apart.
"""

from __future__ import annotations

import re
from types import GenericAlias
from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    create_model,
    field_validator,
)

from .config import ExtractionConfig, ExtractionField, TopicConfig

PROVENANCE_PREFIX: tuple[str, ...] = (
    "topic",
    "case_id",
    "story_id",
    "url",
    "domain",
    "publish_date",
    "media_name",
    "title",
    "source_outlet",
    "source_url",
    "link_category",
)
"""Columns the pipeline derives itself. The model is never asked for these."""

AUDIT_SUFFIX: tuple[str, ...] = (
    "evidence_verified",
    "model",
    "prompt_version",
    "extracted_at",
)
"""Columns recording how a row was produced."""


def case_csv_columns(topic: TopicConfig) -> tuple[str, ...]:
    """Return the exact case-CSV header for *topic*.

    The column list is composed, never hardcoded a second time, so adding an
    extraction field to YAML adds a CSV column with no code change.
    """
    field_names = tuple(field.name for field in topic.extraction.fields)
    columns = PROVENANCE_PREFIX + field_names + AUDIT_SUFFIX

    duplicates = {name for name in columns if columns.count(name) > 1}
    if duplicates:
        raise ValueError(
            f"extraction fields collide with reserved CSV columns: {sorted(duplicates)}"
        )
    return columns


ARTICLE_AUDIT_COLUMNS: tuple[str, ...] = (
    "topic",
    "story_id",
    "url",
    "domain",
    "publish_date",
    "media_name",
    "title",
    "qc_status",
    "qc_reason",
    "dup_of_story_id",
    "fetch_status",
    "source",
    "text_chars",
    "screening_relevant",
    "screening_validation_status",
    "screening_prompt_version",
    "relevant",
    "reject_reason",
)

def _field_schema(field: ExtractionField) -> dict[str, Any]:
    schema: dict[str, Any] = {
        "type": ["string", "null"],
        "description": field.description,
    }
    if field.regex is not None:
        schema["pattern"] = field.regex
    if field.allowed_values is not None:
        schema["enum"] = [*field.allowed_values, None]
    return schema


def build_case_json_schema(extraction: ExtractionConfig) -> dict[str, Any]:
    """Return the strict-mode object schema for one extracted case.

    Strict structured outputs require every property to appear in ``required``;
    configured values remain optional through nullable types.
    """
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {field.name: _field_schema(field) for field in extraction.fields},
        "required": [field.name for field in extraction.fields],
    }


def build_screening_json_schema(extraction: ExtractionConfig) -> dict[str, Any]:
    """Return the exact per-article Stage 1 screening decision schema."""
    del extraction
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "decisions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "story_id": {
                            "type": "string",
                            "description": (
                                "Echo the immutable id of the article this entry describes."
                            ),
                        },
                        "relevant": {
                            "type": "boolean",
                            "description": (
                                "True only when the article meets the supplied criteria."
                            ),
                        },
                    },
                    "required": ["story_id", "relevant"],
                },
            }
        },
        "required": ["decisions"],
    }


def build_response_json_schema(extraction: ExtractionConfig) -> dict[str, Any]:
    """Return a relevant-only detailed-extraction envelope for one packed request."""
    properties = {
        "story_id": {
            "type": "string",
            "description": "Echo the immutable id of the article this result describes.",
        },
        "cases": {
            "type": "array",
            "description": "Explicit high-confidence findings for this relevant article.",
            "items": build_case_json_schema(extraction),
        },
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "results": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": properties,
                    "required": ["story_id", "cases"],
                },
            }
        },
        "required": ["results"],
    }


def _list_of(model: type[BaseModel]) -> Any:
    """Build ``list[model]`` at runtime.

    A subscripted variable is not valid as a static type, so the alias is
    constructed explicitly rather than written as an annotation.
    """
    return GenericAlias(list, (model,))


def build_case_model(extraction: ExtractionConfig) -> type[BaseModel]:
    """Return the model that validates one extracted case.

    Configured fields are optional and nullable so incomplete model output remains auditable.
    """
    definitions: dict[str, Any] = {
        field.name: (
            str | None,
            Field(None, description=field.description),
        )
        for field in extraction.fields
    }
    validators = {
        f"validate_{field.name}_constraints": _field_constraint_validator(field)
        for field in extraction.fields
        if field.regex is not None or field.allowed_values is not None
    }
    return create_model(
        "ExtractedCase",
        __config__=ConfigDict(extra="forbid"),
        __validators__=validators,
        **definitions,
    )


def _field_constraint_validator(field: ExtractionField) -> Any:
    pattern = re.compile(field.regex) if field.regex is not None else None
    allowed_values = set(field.allowed_values) if field.allowed_values is not None else None

    @field_validator(field.name)
    @classmethod
    def validate_constraint(cls, value: str | None) -> str | None:
        if value is None:
            return value
        if pattern is not None and pattern.fullmatch(value) is None:
            raise ValueError(f"{field.name} must match configured regex")
        if allowed_values is not None and value not in allowed_values:
            raise ValueError(f"{field.name} must be an allowed value")
        return value

    return validate_constraint


def _response_model(extraction: ExtractionConfig, *, include_cases: bool) -> type[BaseModel]:
    if not include_cases:
        screening_model = create_model(
            "ScreeningArticle",
            __config__=ConfigDict(extra="forbid"),
            story_id=(str, Field(...)),
            relevant=(bool, Field(...)),
        )
        return create_model(
            "ScreeningResponse",
            __config__=ConfigDict(extra="forbid"),
            decisions=(_list_of(screening_model), Field(default_factory=list)),
        )
    definitions: dict[str, Any] = {
        "story_id": (str, Field(...)),
        "cases": (_list_of(build_case_model(extraction)), Field(default_factory=list)),
    }
    story_model = create_model(
        "ExtractionArticle",
        __config__=ConfigDict(extra="forbid"),
        **definitions,
    )
    return create_model(
        "ExtractionResponse",
        __config__=ConfigDict(extra="forbid"),
        results=(_list_of(story_model), Field(default_factory=list)),
    )


def build_response_model(extraction: ExtractionConfig) -> type[BaseModel]:
    """Return the model that validates one complete extraction response."""
    return _response_model(extraction, include_cases=True)


def build_screening_model(extraction: ExtractionConfig) -> type[BaseModel]:
    """Return the model that validates exact per-article screening decisions."""
    return _response_model(extraction, include_cases=False)
