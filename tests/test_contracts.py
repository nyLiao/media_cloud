from __future__ import annotations

import sqlite3

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
from mc_pipeline.db import init_db


@pytest.fixture
def topic():
    return load_config().topics["revolving_door_ca"]


@pytest.fixture
def extraction(topic):
    return topic.extraction


def _individual(**overrides):
    payload = {
        "case_type": "individual",
        "person_name": "Ada Example",
        "cohort_name": None,
        "cohort_size": None,
        "cohort_period": None,
        "previous_org": "Government of British Columbia",
        "previous_role": "Minister",
        "previous_sector": "public",
        "previous_year": 2019,
        "current_org": "Example Lobbying",
        "current_role": "Consultant lobbyist",
        "current_sector": "private",
        "current_year": 2021,
        "direction": "public_to_private",
        "jurisdiction": "British Columbia",
        "expected_resolvable": "yes",
        "status": "reported",
        "claim": "Ada moved from cabinet to lobbying.",
        "evidence_quote": "Ada Example joined Example Lobbying in 2021.",
    }
    return payload | overrides


def _cohort(**overrides):
    cohort_defaults = {
        "case_type": "cohort",
        "person_name": None,
        "cohort_name": "Lobbyists at fundraisers",
        "cohort_size": 166,
        "cohort_period": "2019-2023",
        "previous_org": None,
        "current_org": None,
    }
    return _individual(**(cohort_defaults | overrides))


# --- CSV column composition (C5) ---------------------------------------------


def test_csv_columns_compose_prefix_config_fields_and_suffix(topic):
    columns = case_csv_columns(topic)
    field_names = tuple(field.name for field in topic.extraction.fields)

    assert columns == PROVENANCE_PREFIX + field_names + AUDIT_SUFFIX
    assert columns[: len(PROVENANCE_PREFIX)] == PROVENANCE_PREFIX
    assert columns[-len(AUDIT_SUFFIX) :] == AUDIT_SUFFIX


def test_adding_a_config_field_adds_a_csv_column(topic):
    before = case_csv_columns(topic)
    extended = topic.model_copy(deep=True)
    extended.extraction.fields.append(
        type(extended.extraction.fields[0])(
            name="oversight_body", type="string", required=False, description="Reviewing body."
        )
    )

    after = case_csv_columns(extended)
    assert len(after) == len(before) + 1
    assert "oversight_body" in after
    assert after[-len(AUDIT_SUFFIX) :] == AUDIT_SUFFIX


def test_reserved_column_collision_is_rejected(topic):
    clashing = topic.model_copy(deep=True)
    clashing.extraction.fields.append(
        type(clashing.extraction.fields[0])(
            name="link_category", type="string", required=False, description="Clashes."
        )
    )

    with pytest.raises(ValueError, match="collide with reserved CSV columns"):
        case_csv_columns(clashing)


def test_every_extraction_field_has_a_cases_column(topic, tmp_path):
    """Guards the config / schema / CSV drift that C5 fixed."""
    connection = init_db(tmp_path / "pipeline.db")
    try:
        case_columns = {row[1] for row in connection.execute("PRAGMA table_info(cases)")}
    finally:
        connection.close()

    field_names = {field.name for field in topic.extraction.fields}
    missing = sorted(field_names - case_columns)
    assert not missing, f"extraction fields with no cases column: {missing}"


def test_pipeline_derived_columns_are_never_requested_from_the_model(topic):
    field_names = {field.name for field in topic.extraction.fields}
    assert not field_names & set(PROVENANCE_PREFIX)
    assert not field_names & set(AUDIT_SUFFIX)


# --- JSON Schema strict-mode semantics (C6) ---------------------------------


def test_strict_schema_marks_every_property_required(extraction):
    schema = build_case_json_schema(extraction)
    assert schema["required"] == [field.name for field in extraction.fields]
    assert set(schema["required"]) == set(schema["properties"])
    assert schema["additionalProperties"] is False


def test_optional_fields_are_nullable_unions_not_absent_from_required(extraction):
    schema = build_case_json_schema(extraction)

    assert schema["properties"]["case_type"]["type"] == "string"
    assert schema["properties"]["person_name"]["type"] == ["string", "null"]
    assert schema["properties"]["cohort_size"]["type"] == ["integer", "null"]
    assert "person_name" in schema["required"]


def test_optional_enum_admits_null_and_required_enum_does_not(extraction):
    properties = build_case_json_schema(extraction)["properties"]

    assert None in properties["direction"]["enum"]
    assert properties["expected_resolvable"]["enum"] == ["yes", "partial", "no"]
    assert None not in properties["expected_resolvable"]["enum"]


def test_response_schema_keys_results_by_story_id(extraction):
    schema = build_response_json_schema(extraction)
    item = schema["properties"]["results"]["items"]

    assert item["required"] == ["story_id", "relevant", "reject_reason", "cases"]
    assert item["properties"]["story_id"]["type"] == "string"
    assert item["properties"]["cases"]["items"] == build_case_json_schema(extraction)


# --- Pydantic validation, including conditional rules (C6) -------------------


def test_individual_and_cohort_payloads_both_validate(extraction):
    model = build_case_model(extraction)

    assert model.model_validate(_individual()).person_name == "Ada Example"
    cohort = model.model_validate(_cohort())
    assert cohort.person_name is None
    assert cohort.cohort_size == 166


def test_cohort_payload_needs_no_organizations(extraction):
    """previous_org/current_org are required for individuals only, per the GC011 shape."""
    model = build_case_model(extraction)
    cohort = model.model_validate(_cohort())
    assert cohort.previous_org is None
    assert cohort.current_org is None


@pytest.mark.parametrize("missing", ["person_name", "previous_org", "current_org"])
def test_individual_payload_requires_its_conditional_fields(extraction, missing):
    model = build_case_model(extraction)
    with pytest.raises(ValidationError, match=f"{missing} is required when case_type"):
        model.model_validate(_individual(**{missing: None}))


def test_cohort_payload_requires_a_cohort_name(extraction):
    model = build_case_model(extraction)
    with pytest.raises(ValidationError, match="cohort_name is required when case_type"):
        model.model_validate(_cohort(cohort_name=None))


def test_blank_string_does_not_satisfy_a_conditional_requirement(extraction):
    model = build_case_model(extraction)
    with pytest.raises(ValidationError, match="person_name is required"):
        model.model_validate(_individual(person_name="   "))


def test_unconditionally_required_field_rejects_null(extraction):
    model = build_case_model(extraction)
    with pytest.raises(ValidationError):
        model.model_validate(_individual(expected_resolvable=None))


def test_out_of_enum_value_is_rejected_before_it_can_reach_sqlite(extraction, tmp_path):
    """C3: Pydantic rejects the value so no CHECK constraint can abort a batch."""
    model = build_case_model(extraction)
    with pytest.raises(ValidationError):
        model.model_validate(_individual(expected_resolvable="unknown"))

    connection = init_db(tmp_path / "pipeline.db")
    try:
        connection.execute("INSERT INTO stories(story_id, raw_json) VALUES ('s', '{}')")
        extraction_id = connection.execute(
            "INSERT INTO story_extractions(topic, story_id, prompt_version) VALUES ('t', 's', 'v1')"
        ).lastrowid
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO cases(case_id, story_extraction_id, case_type,
                                  expected_resolvable, status, claim)
                VALUES ('c', ?, 'individual', 'unknown', 'reported', 'a claim')
                """,
                (extraction_id,),
            )
    finally:
        connection.close()


def test_unknown_field_is_rejected(extraction):
    model = build_case_model(extraction)
    with pytest.raises(ValidationError):
        model.model_validate(_individual(invented_field="x"))


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
                    "reject_reason": "No named individual.",
                    "cases": [],
                },
            ]
        }
    )

    assert [entry.story_id for entry in response.results] == ["s1", "s2"]
    assert len(response.results[0].cases) == 1
    assert response.results[1].cases == []
