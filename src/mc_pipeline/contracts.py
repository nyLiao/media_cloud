"""Shapes derived from topic extraction configuration.

``extraction.fields`` in ``config/topics.yaml`` is the single declaration of what the
LLM must return. Every downstream shape is derived from it here -- the JSON Schema
sent to the provider, the model that validates the response, and the CSV column
order -- so the three cannot drift apart.
"""

from __future__ import annotations

from collections.abc import Callable
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


def _json_type(field: ExtractionField) -> str:
    return "integer" if field.field_type == "integer" else "string"


def _field_schema(field: ExtractionField) -> dict[str, Any]:
    base_type = _json_type(field)
    schema: dict[str, Any] = {
        "type": base_type if field.required else [base_type, "null"],
        "description": field.description,
    }
    if field.field_type == "enum":
        values: list[str | None] = list(field.values or ())
        if not field.required:
            values.append(None)
        schema["enum"] = values
    return schema


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
    """Return the Python annotation for *field*.

    Enum membership is enforced by :func:`_enum_validator` rather than by a
    ``Literal``: the values are only known at runtime, and the JSON Schema this
    module emits already carries the ``enum`` constraint for the provider.
    """
    inner: Any = int if field.field_type == "integer" else str
    return inner if field.required else inner | None


def _enum_validator(extraction: ExtractionConfig) -> Callable[[Any], Any]:
    """Reject values outside a configured enum before they can reach SQLite."""
    allowed = {
        field.name: tuple(field.values or ())
        for field in extraction.fields
        if field.field_type == "enum"
    }

    def validate(instance: Any) -> Any:
        for name, values in allowed.items():
            value = getattr(instance, name, None)
            if value is not None and value not in values:
                raise ValueError(f"{name} must be one of {list(values)}, got {value!r}")
        return instance

    return validate


def _conditional_validator(extraction: ExtractionConfig) -> Callable[[Any], Any]:
    """Enforce requirements that hold only for some discriminator variants."""
    discriminator = extraction.discriminator
    rules = {
        variant: tuple(names) for variant, names in extraction.required_by_discriminator.items()
    }

    def validate(instance: Any) -> Any:
        variant = getattr(instance, str(discriminator), None)
        for name in rules.get(str(variant), ()):
            value = getattr(instance, name, None)
            if value is None or (isinstance(value, str) and not value.strip()):
                raise ValueError(f"{name} is required when {discriminator} is {variant!r}")
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

    This is where ``required``, enum membership, and the conditional rules are
    actually enforced, because a strict JSON Schema cannot express the last of them.
    """
    definitions: dict[str, Any] = {
        field.name: (
            _annotation(field),
            Field(... if field.required else None, description=field.description),
        )
        for field in extraction.fields
    }

    validators: dict[str, Any] = {
        "validate_enum_membership": model_validator(mode="after")(_enum_validator(extraction))
    }
    if extraction.discriminator is not None:
        validators["validate_conditional_requirements"] = model_validator(mode="after")(
            _conditional_validator(extraction)
        )

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
