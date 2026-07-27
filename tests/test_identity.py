from __future__ import annotations

from datetime import date

import pytest

from mc_pipeline.identity import (
    build_case_id,
    canonical_json,
    semantic_query_hash,
    sha256_hex,
    window_fingerprint,
)

SEARCH = {
    "query": '"revolving door"',
    "platform": "onlinenews-mediacloud",
    "collection_ids": [34411583],
    "source_ids": [],
    "languages": ["en"],
}
WINDOW = {"window_start": date(2021, 1, 1), "window_end": date(2021, 1, 31)}


def test_canonical_json_is_order_independent_and_compact():
    assert canonical_json({"b": 1, "a": 2}) == '{"a":2,"b":1}'
    assert canonical_json({"a": 2, "b": 1}) == canonical_json({"b": 1, "a": 2})


def test_canonical_json_serializes_dates_and_rejects_unsupported_types():
    assert canonical_json({"d": date(2021, 1, 1)}) == '{"d":"2021-01-01"}'
    with pytest.raises(TypeError, match="unsupported type"):
        sha256_hex({"session": object()})


def test_fingerprint_is_stable_across_calls():
    assert window_fingerprint(**SEARCH, **WINDOW) == window_fingerprint(**SEARCH, **WINDOW)


def test_fingerprint_ignores_ordering_of_scope_lists():
    """Reordering collection ids cannot change the matched set, so it must not re-spend quota."""
    reordered = {**SEARCH, "collection_ids": [34411583, 111], "languages": ["en", "fr"]}
    shuffled = {**SEARCH, "collection_ids": [111, 34411583], "languages": ["fr", "en"]}
    assert window_fingerprint(**reordered, **WINDOW) == window_fingerprint(**shuffled, **WINDOW)


@pytest.mark.parametrize(
    "override",
    [
        {"query": '"revolving door" OR lobbyist'},
        {"platform": "onlinenews-waybackmachine"},
        {"collection_ids": [34411583, 999]},
        {"source_ids": [42]},
        {"languages": ["en", "fr"]},
    ],
)
def test_semantic_changes_invalidate_the_window(override):
    assert window_fingerprint(**{**SEARCH, **override}, **WINDOW) != window_fingerprint(
        **SEARCH, **WINDOW
    )


def test_window_bounds_are_part_of_the_fingerprint():
    other = {"window_start": date(2021, 2, 1), "window_end": date(2021, 2, 28)}
    assert window_fingerprint(**SEARCH, **other) != window_fingerprint(**SEARCH, **WINDOW)


def test_contract_version_bump_invalidates_the_window():
    assert window_fingerprint(**SEARCH, **WINDOW, contract_version=2) != window_fingerprint(
        **SEARCH, **WINDOW
    )


def test_fingerprint_excludes_request_controls():
    """page_size changes paging, never which stories match, so it is not a cache key.

    Guards the quota invariant: the fingerprint accepts no request-control argument,
    so tuning page_size cannot invalidate completed windows.
    """
    with pytest.raises(TypeError):
        window_fingerprint(**SEARCH, **WINDOW, page_size=500)  # type: ignore[call-arg]


def test_semantic_query_hash_is_window_independent():
    assert semantic_query_hash(**SEARCH) == semantic_query_hash(**SEARCH)
    assert semantic_query_hash(**SEARCH) != window_fingerprint(**SEARCH, **WINDOW)


def test_case_id_is_deterministic_and_ordinal_padded():
    identifier = build_case_id(
        topic="revolving_door_ca", story_id="story-1", prompt_version="v1", ordinal=0
    )
    assert identifier == "revolving_door_ca:story-1:v1:00"
    assert identifier == build_case_id(
        topic="revolving_door_ca", story_id="story-1", prompt_version="v1", ordinal=0
    )


def test_case_ids_differ_by_topic_prompt_version_and_ordinal():
    base = {"topic": "revolving_door_ca", "story_id": "story-1", "prompt_version": "v1"}
    identifiers = {
        build_case_id(**base, ordinal=0),
        build_case_id(**base, ordinal=1),
        build_case_id(**{**base, "topic": "patronage_ca"}, ordinal=0),
        build_case_id(**{**base, "prompt_version": "v2"}, ordinal=0),
    }
    assert len(identifiers) == 4


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"topic": "bad:topic"}, "must not contain"),
        ({"story_id": ""}, "must not be empty"),
        ({"ordinal": -1}, "non-negative"),
    ],
)
def test_case_id_rejects_ambiguous_components(kwargs, match):
    base = {
        "topic": "revolving_door_ca",
        "story_id": "story-1",
        "prompt_version": "v1",
        "ordinal": 0,
    }
    with pytest.raises(ValueError, match=match):
        build_case_id(**{**base, **kwargs})
