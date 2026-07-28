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
        "cohort_name": None,
        "private_org": "Example Strategies",
        "private_time": "2024",
        "public_org": "Government of Canada",
        "public_time": "2021-2024",
        "jurisdiction": "Canada",
        **overrides,
    }


def _cohort(**overrides):
    return _individual(
        person_name=None,
        cohort_name="Former ministerial staff",
        **overrides,
    )


def test_case_csv_columns_follow_the_seven_field_contract():
    topic = load_config().topics["revolving_door_ca"]

    assert (
        case_csv_columns(topic)
        == PROVENANCE_PREFIX
        + (
            "person_name",
            "cohort_name",
            "private_org",
            "private_time",
            "public_org",
            "public_time",
            "jurisdiction",
        )
        + AUDIT_SUFFIX
    )


def test_case_schema_is_strict_and_uses_string_times(extraction):
    schema = build_case_json_schema(extraction)

    assert schema["additionalProperties"] is False
    assert schema["required"] == [field.name for field in extraction.fields]
    assert schema["properties"]["private_time"]["type"] == ["string", "null"]
    assert schema["properties"]["public_time"]["type"] == ["string", "null"]
    assert schema["properties"]["private_org"]["type"] == "string"


def test_response_schema_embeds_case_schema(extraction):
    schema = build_response_json_schema(extraction)
    item = schema["properties"]["results"]["items"]

    assert item["properties"]["cases"]["items"] == build_case_json_schema(extraction)


def test_individual_and_cohort_payloads_validate(extraction):
    model = build_case_model(extraction)

    assert model.model_validate(_individual()).person_name == "Ada Example"
    assert model.model_validate(_cohort()).cohort_name == "Former ministerial staff"


@pytest.mark.parametrize(
    "payload",
    [
        _individual(person_name=None, cohort_name=None),
        _individual(cohort_name="Former staff"),
        _individual(person_name="   "),
    ],
)
def test_model_requires_exactly_one_nonblank_person_or_cohort(extraction, payload):
    model = build_case_model(extraction)

    with pytest.raises(ValidationError, match="exactly one"):
        model.model_validate(payload)


@pytest.mark.parametrize("field", ["private_org", "public_org", "jurisdiction"])
def test_model_requires_transition_organizations_and_jurisdiction(extraction, field):
    model = build_case_model(extraction)

    with pytest.raises(ValidationError):
        model.model_validate(_individual(**{field: None}))

    with pytest.raises(ValidationError, match=f"{field} must be provided"):
        model.model_validate(_individual(**{field: "   "}))


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
                    "relevant": True,
                    "reject_reason": None,
                    "cases": [_individual()],
                },
                {
                    "story_id": "s2",
                    "relevant": False,
                    "reject_reason": "No explicit transition.",
                    "cases": [],
                },
            ]
        }
    )

    assert [entry.story_id for entry in response.results] == ["s1", "s2"]
