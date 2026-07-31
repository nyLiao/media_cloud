from __future__ import annotations

import pytest
from pydantic import ValidationError

from mc_pipeline.config import load_config
from mc_pipeline.contracts import (
    AUDIT_SUFFIX,
    PROVENANCE_PREFIX,
    build_case_json_schema,
    build_case_model,
    build_response_json_schema,
    build_response_model,
    case_csv_columns,
)


@pytest.fixture
def extraction():
    return load_config().topics["revolving_door_ca"].extraction


def _individual(**overrides):
    return {
        "person_name": "Ada Example",
        "private_org": "Example Strategies",
        "private_time": "2024-2024",
        "public_org": "Government of Canada",
        "public_time": "2021-2024",
        "jurisdiction": "Ontario",
        **overrides,
    }


def test_case_csv_columns_follow_configured_field_order():
    topic = load_config().topics["revolving_door_ca"]

    assert (
        case_csv_columns(topic)
        == PROVENANCE_PREFIX
        + (
            "person_name",
            "private_org",
            "private_time",
            "public_org",
            "public_time",
            "jurisdiction",
        )
        + AUDIT_SUFFIX
    )


def test_case_schema_is_strict_and_exposes_configured_constraints(extraction):
    schema = build_case_json_schema(extraction)

    assert schema["additionalProperties"] is False
    assert schema["required"] == [field.name for field in extraction.fields]
    assert schema["properties"]["private_time"]["type"] == ["string", "null"]
    assert schema["properties"]["public_time"]["type"] == ["string", "null"]
    assert schema["properties"]["private_time"]["pattern"] == r"^\d{4}(?:-\d{4})?$"
    assert schema["properties"]["public_time"]["pattern"] == r"^\d{4}(?:-\d{4})?$"
    assert schema["properties"]["private_org"]["type"] == ["string", "null"]
    assert schema["properties"]["jurisdiction"]["enum"] == [
        "Alberta",
        "British Columbia",
        "Manitoba",
        "New Brunswick",
        "Newfoundland and Labrador",
        "Nova Scotia",
        "Ontario",
        "Prince Edward Island",
        "Quebec",
        "Saskatchewan",
        None,
    ]


def test_response_schema_embeds_case_schema(extraction):
    schema = build_response_json_schema(extraction)
    item = schema["properties"]["results"]["items"]

    assert item["properties"]["cases"]["items"] == build_case_json_schema(extraction)
    assert item["required"] == ["story_id", "cases"]
    assert "relevant" not in item["properties"]
    assert "reject_reason" not in item["properties"]


def test_model_accepts_missing_null_and_valid_configured_values(extraction):
    model = build_case_model(extraction)

    assert model.model_validate(_individual()).person_name == "Ada Example"
    incomplete = model.model_validate({"person_name": None, "private_org": ""})
    assert incomplete.person_name is None
    assert incomplete.private_org == ""
    assert incomplete.public_org is None


@pytest.mark.parametrize("jurisdiction", [None, "Ontario", "Quebec"])
def test_model_accepts_null_or_configured_jurisdiction(extraction, jurisdiction):
    model = build_case_model(extraction)

    assert model.model_validate(_individual(jurisdiction=jurisdiction)).jurisdiction == jurisdiction


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("private_time", "May 2024"),
        ("public_time", "2024-05-01"),
        ("private_time", "Senior adviser"),
        ("jurisdiction", "Canada"),
        ("jurisdiction", "federal"),
        ("jurisdiction", "Toronto"),
    ],
)
def test_model_rejects_values_outside_configured_constraints(extraction, field, value):
    with pytest.raises(ValidationError):
        build_case_model(extraction).model_validate(_individual(**{field: value}))


def test_model_rejects_unknown_fields(extraction):
    model = build_case_model(extraction)

    with pytest.raises(ValidationError):
        model.model_validate(_individual(case_type="individual"))


def test_response_model_validates_a_packed_batch(extraction):
    model = build_response_model(extraction)
    response = model.model_validate(
        {
            "results": [
                {
                    "story_id": "s1",
                    "cases": [_individual()],
                },
            ]
        }
    )

    assert [entry.story_id for entry in response.results] == ["s1"]
