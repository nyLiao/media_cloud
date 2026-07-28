"""Shared result types for pipeline stages and review commands."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class StageSummary:
    """Bounded outcome counts for one pipeline-stage invocation."""

    topic: str
    processed: int
    succeeded: int
    skipped: int
    failed: int

    def __str__(self) -> str:
        return (
            f"topic={self.topic} processed={self.processed} succeeded={self.succeeded} "
            f"skipped={self.skipped} failed={self.failed}"
        )


@dataclass(frozen=True)
class SearchReviewSummary:
    """Reconciliation totals and output path for a search review export."""

    topic: str
    windows: int
    completed_windows: int
    incomplete_windows: int
    hits: int
    distinct_stories: int
    duplicate_hits: int
    output_path: Path

    def __str__(self) -> str:
        return (
            f"topic={self.topic} windows={self.windows} completed={self.completed_windows} "
            f"incomplete={self.incomplete_windows} hits={self.hits} "
            f"distinct_stories={self.distinct_stories} duplicate_hits={self.duplicate_hits}"
        )
