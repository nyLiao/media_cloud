from __future__ import annotations

from mc_pipeline.config import load_config
from mc_pipeline.prompt import build_extraction_prompt


def test_prompt_is_short_and_states_the_non_inference_contract():
    extraction = load_config().topics["revolving_door_ca"].extraction
    prompt = build_extraction_prompt(extraction)

    assert len(prompt) < 700
    assert "explicit, high-confidence" in prompt
    assert "Omit uncertain records" in prompt
    assert "never infer" in prompt
    assert "null for unstated optional values" in prompt
    assert "Exactly one of `person_name` or `cohort_name`" in prompt


def test_prompt_lists_only_the_strict_seven_field_schema():
    extraction = load_config().topics["revolving_door_ca"].extraction
    prompt = build_extraction_prompt(extraction)

    for field in [
        "person_name",
        "cohort_name",
        "private_org",
        "private_time",
        "public_org",
        "public_time",
        "jurisdiction",
    ]:
        assert f'"{field}"' in prompt
    assert "case_type" not in prompt
