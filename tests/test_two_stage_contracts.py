from __future__ import annotations

import pytest
from pydantic import ValidationError

from mc_pipeline.config import load_config
from mc_pipeline.contracts import (
    build_case_json_schema,
    build_case_model,
    build_response_json_schema,
    build_response_model,
    build_screening_json_schema,
    build_screening_model,
)


@pytest.fixture
def extraction():
    return load_config().topics["revolving_door_ca"].extraction


def _case(**overrides):
    return {
        "person_name": "Ada Example",
        "private_org": "Example Strategies",
        "private_time": "2024-2024",
        "public_org": "Government of Canada",
        "public_time": "2021-2024",
        "jurisdiction": "Ontario",
        **overrides,
    }


def test_stage_one_schema_is_exact_boolean_decisions(extraction):
    schema = build_screening_json_schema(extraction)

    assert set(schema["properties"]) == {"decisions"}
    item = schema["properties"]["decisions"]["items"]
    assert item["required"] == ["story_id", "relevant"]
    assert item["properties"]["relevant"] == {
        "type": "boolean",
        "description": "True only when the article meets the supplied criteria.",
    }
    assert "reject_reason" not in item["properties"]
    assert "cases" not in item["properties"]


def test_stage_two_schema_has_relevant_only_results_and_cases(extraction):
    schema = build_response_json_schema(extraction)
    item = schema["properties"]["results"]["items"]

    assert item["required"] == ["story_id", "cases"]
    assert "relevant" not in item["properties"]
    assert "reject_reason" not in item["properties"]
    assert item["properties"]["cases"]["items"] == build_case_json_schema(extraction)


def test_case_schema_exposes_configured_constraints(extraction):
    schema = build_case_json_schema(extraction)

    assert schema["properties"]["private_time"]["type"] == ["string", "null"]
    assert schema["properties"]["public_time"]["type"] == ["string", "null"]
    assert schema["properties"]["private_time"]["pattern"] == r"^\d{4}(?:-\d{4})?$"
    assert schema["properties"]["public_time"]["pattern"] == r"^\d{4}(?:-\d{4})?$"
    assert schema["properties"]["jurisdiction"]["type"] == ["string", "null"]
    assert "Ontario" in schema["properties"]["jurisdiction"]["enum"]
    assert None in schema["properties"]["jurisdiction"]["enum"]


@pytest.mark.parametrize("jurisdiction", [None, "Ontario", "Quebec"])
def test_case_model_accepts_null_or_allowed_jurisdiction(extraction, jurisdiction):
    assert build_case_model(extraction).model_validate(
        _case(jurisdiction=jurisdiction)
    ).jurisdiction == (jurisdiction)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("private_time", "May 2024"),
        ("public_time", "2024-05-01"),
        ("jurisdiction", "Canada"),
        ("jurisdiction", "federal"),
        ("jurisdiction", "Toronto"),
    ],
)
def test_case_model_rejects_values_outside_configured_constraints(extraction, field, value):
    with pytest.raises(ValidationError):
        build_case_model(extraction).model_validate(_case(**{field: value}))


def test_response_models_require_exact_stage_one_and_stage_two_shapes(extraction):
    screening = build_screening_model(extraction).model_validate(
        {"decisions": [{"story_id": "s1", "relevant": True}]}
    )
    assert screening.decisions[0].relevant is True

    extracted = build_response_model(extraction).model_validate(
        {
            "results": [
                {"story_id": "s1", "cases": [_case()]}
            ]
        }
    )
    assert extracted.results[0].story_id == "s1"

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        build_response_model(extraction).model_validate(
            {
                "results": [
                    {"story_id": "s1", "relevant": True, "cases": []}
                ]
            }
        )
