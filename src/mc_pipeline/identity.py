"""Canonical hashing and deterministic identifiers.

Every cache key, provenance hash, and generated identifier is produced here so the
same inputs always yield the same value across processes, machines, and reruns.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from datetime import date
from typing import Any

SEARCH_CONTRACT_VERSION = 1
"""Bump only when a change in search semantics must invalidate cached windows."""

CASE_ID_SEPARATOR = ":"


def _json_default(value: Any) -> str:
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(f"Refusing to hash unsupported type: {type(value).__name__}")


def canonical_json(payload: Any) -> str:
    """Serialize *payload* so that equal values always produce identical text."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=_json_default)


def sha256_hex(payload: Any) -> str:
    """Return the SHA-256 hex digest of the canonical JSON form of *payload*."""
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def _semantic_search_inputs(
    *,
    query: str,
    platform: str,
    collection_ids: Sequence[int],
    source_ids: Sequence[int],
    languages: Sequence[str],
    contract_version: int,
) -> dict[str, Any]:
    """Build the inputs that determine *which* stories a query matches.

    Sequences are sorted so that merely reordering ``collection_ids`` in YAML does
    not invalidate a cached window -- order cannot change the matched set.
    """
    return {
        "contract_version": contract_version,
        "query": query,
        "platform": platform,
        "collection_ids": sorted(collection_ids),
        "source_ids": sorted(source_ids),
        "languages": sorted(languages),
    }


def semantic_query_hash(
    *,
    query: str,
    platform: str,
    collection_ids: Sequence[int],
    source_ids: Sequence[int],
    languages: Sequence[str],
    contract_version: int = SEARCH_CONTRACT_VERSION,
) -> str:
    """Hash the topic-level search semantics, independent of any date window."""
    return sha256_hex(
        _semantic_search_inputs(
            query=query,
            platform=platform,
            collection_ids=collection_ids,
            source_ids=source_ids,
            languages=languages,
            contract_version=contract_version,
        )
    )


def window_fingerprint(
    *,
    query: str,
    platform: str,
    collection_ids: Sequence[int],
    source_ids: Sequence[int],
    languages: Sequence[str],
    window_start: date,
    window_end: date,
    contract_version: int = SEARCH_CONTRACT_VERSION,
) -> str:
    """Quota-cache key for one search window.

    Covers semantic inputs plus the window bounds and nothing else. Request
    controls such as ``page_size`` and ``max_pages_per_window`` are deliberately
    excluded: they change how results are paged, never which stories match, so
    including them would let a harmless config tweak invalidate completed windows
    and re-spend Media Cloud quota.
    """
    payload = _semantic_search_inputs(
        query=query,
        platform=platform,
        collection_ids=collection_ids,
        source_ids=source_ids,
        languages=languages,
        contract_version=contract_version,
    )
    payload["window_start"] = window_start
    payload["window_end"] = window_end
    return sha256_hex(payload)


def build_case_id(*, topic: str, story_id: str, prompt_version: str, ordinal: int) -> str:
    """Return the deterministic identifier for one extracted case.

    Never ask the model for this value: it must stay stable across reruns and
    unique across topics and prompt versions. Components may not contain the
    separator, which keeps the encoding unambiguous.
    """
    if ordinal < 0:
        raise ValueError(f"case ordinal must be non-negative, got {ordinal}")

    components = {"topic": topic, "story_id": story_id, "prompt_version": prompt_version}
    for name, value in components.items():
        if not value:
            raise ValueError(f"case id component {name!r} must not be empty")
        if CASE_ID_SEPARATOR in value:
            raise ValueError(
                f"case id component {name!r} must not contain {CASE_ID_SEPARATOR!r}: {value!r}"
            )

    return CASE_ID_SEPARATOR.join((topic, story_id, prompt_version, f"{ordinal:02d}"))
