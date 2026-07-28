"""Shapes derived from topic extraction configuration.

``extraction.fields`` in ``config/topics.yaml`` is the single declaration of what the
LLM must return. Every downstream shape is derived from it here -- the JSON Schema
sent to the provider, the model that validates the response, and the CSV column
order -- so the three cannot drift apart.
"""

from __future__ import annotations

from types import GenericAlias
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, create_model, model_validator

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
    "relevant",
    "reject_reason",
)


def _field_schema(field: ExtractionField) -> dict[str, Any]:
    return {
        "type": "string" if field.required else ["string", "null"],
        "description": field.description,
    }


def build_case_json_schema(extraction: ExtractionConfig) -> dict[str, Any]:
    """Return the strict-mode object schema for one extracted case.

    Strict structured outputs require *every* property to appear in ``required``;
    optionality is expressed as a nullable type union instead. So
    ``ExtractionField.required`` controls nullability here and real requiredness in
    :func:`build_case_model`. Mapping it onto this ``required`` array would make the
    provider reject the schema.
    """
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {field.name: _field_schema(field) for field in extraction.fields},
        "required": [field.name for field in extraction.fields],
    }


def build_response_json_schema(extraction: ExtractionConfig) -> dict[str, Any]:
    """Return the envelope schema for one packed multi-article request."""
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "results": {
                "type": "array",
                "description": "Exactly one entry per article supplied in this request.",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "story_id": {
                            "type": "string",
                            "description": (
                                "Echo the id attribute of the article this entry describes."
                            ),
                        },
                        "relevant": {
                            "type": "boolean",
                            "description": "Whether the article satisfies the relevance criteria.",
                        },
                        "reject_reason": {
                            "type": ["string", "null"],
                            "description": "Why the article was rejected; null when relevant.",
                        },
                        "cases": {
                            "type": "array",
                            "description": "Findings for this article; empty when not relevant.",
                            "items": build_case_json_schema(extraction),
                        },
                    },
                    "required": ["story_id", "relevant", "reject_reason", "cases"],
                },
            }
        },
        "required": ["results"],
    }


def _annotation(field: ExtractionField) -> Any:
    return str if field.required else str | None


def _exactly_one_validator(extraction: ExtractionConfig) -> Any:
    """Require nonblank required values and one configured identifier."""

    def validate(instance: Any) -> Any:
        for field in extraction.fields:
            if field.required and not getattr(instance, field.name).strip():
                raise ValueError(f"{field.name} must be provided")

        populated = [
            name
            for name in extraction.exactly_one_of
            if (value := getattr(instance, name)) is not None and value.strip()
        ]
        if len(populated) != 1:
            raise ValueError(f"exactly one of {extraction.exactly_one_of} must be provided")
        return instance

    return validate


def _list_of(model: type[BaseModel]) -> Any:
    """Build ``list[model]`` at runtime.

    A subscripted variable is not valid as a static type, so the alias is
    constructed explicitly rather than written as an annotation.
    """
    return GenericAlias(list, (model,))


def build_case_model(extraction: ExtractionConfig) -> type[BaseModel]:
    """Return the model that validates one extracted case.

    This is where nullability and the exactly-one identifier rule are enforced.
    """
    definitions: dict[str, Any] = {
        field.name: (
            _annotation(field),
            Field(... if field.required else None, description=field.description),
        )
        for field in extraction.fields
    }

    validators: dict[str, Any] = {
        "validate_exactly_one_identifier": model_validator(mode="after")(
            _exactly_one_validator(extraction)
        )
    }

    return create_model(
        "ExtractedCase",
        __config__=ConfigDict(extra="forbid"),
        __validators__=validators,
        **definitions,
    )


def build_response_model(extraction: ExtractionConfig) -> type[BaseModel]:
    """Return the model that validates one packed multi-article response."""
    case_model = build_case_model(extraction)

    story_model = create_model(
        "StoryExtraction",
        __config__=ConfigDict(extra="forbid"),
        story_id=(str, Field(...)),
        relevant=(bool, Field(...)),
        reject_reason=(str | None, Field(None)),
        cases=(_list_of(case_model), Field(default_factory=list)),
    )

    return create_model(
        "ExtractionResponse",
        __config__=ConfigDict(extra="forbid"),
        results=(_list_of(story_model), Field(default_factory=list)),
    )
