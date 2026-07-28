"""Prompt construction for strict transition extraction."""

from __future__ import annotations

import json

from .config import ExtractionConfig


def build_extraction_prompt(extraction: ExtractionConfig) -> str:
    """Return the compact instruction block for one extraction request."""
    fields = {
        field.name: "string" if field.required else "string|null" for field in extraction.fields
    }
    schema = json.dumps(fields, separators=(",", ":"))
    identifiers = " or ".join(f"`{name}`" for name in extraction.exactly_one_of)
    return (
        "Extract only explicit, high-confidence public/private transitions. "
        "Omit uncertain records; never infer facts. "
        "Use null for unstated optional values. "
        f"Exactly one of {identifiers} must be non-null. "
        "Return only records matching this strict schema: "
        f"{schema}"
    )
