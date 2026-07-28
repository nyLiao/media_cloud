"""Shared result types for pipeline stages and review commands."""

from __future__ import annotations

import platform as python_platform
import subprocess
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from .config import AppConfig
from .identity import canonical_json, sha256_hex


@dataclass(frozen=True)
class StageSummary:
    """Bounded outcome counts for one pipeline-stage invocation."""

    topic: str
    processed: int
    succeeded: int
    skipped: int
    failed: int
    request_details: tuple[str, ...] = ()

    def __str__(self) -> str:
        summary = (
            f"topic={self.topic} processed={self.processed} succeeded={self.succeeded} "
            f"skipped={self.skipped} failed={self.failed}"
        )
        if not self.request_details:
            return summary
        return "\n".join((*self.request_details, summary))


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


@dataclass(frozen=True)
class DedupReviewSummary:
    """Reconciliation totals and output path for a Stage 2 review export."""

    topic: str
    stories: int
    decided: int
    accepted: int
    duplicates: int
    rejected: int
    undecided: int
    duplicate_urls: int
    duplicate_titles: int
    sports_titles: int
    output_path: Path

    def __str__(self) -> str:
        return (
            f"topic={self.topic} stories={self.stories} decided={self.decided} "
            f"accepted={self.accepted} duplicates={self.duplicates} "
            f"rejected={self.rejected} undecided={self.undecided} "
            f"duplicate_urls={self.duplicate_urls} duplicate_titles={self.duplicate_titles} "
            f"sports_titles={self.sports_titles}"
        )


@dataclass(frozen=True)
class FetchReviewSummary:
    """Bounded Stage 3 review export totals and destination."""

    topic: str
    limit: int
    exported: int
    output_path: Path

    def __str__(self) -> str:
        return f"topic={self.topic} limit={self.limit} exported={self.exported}"


def package_version(package: str) -> str:
    """Return an installed package version without failing provenance capture."""
    try:
        return version(package)
    except PackageNotFoundError:
        return "unknown"


def git_revision() -> str:
    """Return the current revision, noting uncommitted work when present."""
    try:
        revision = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return "uncommitted"

    dirty = subprocess.run(
        ["git", "status", "--short"],
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    ).stdout.strip()
    return f"{revision}-dirty" if dirty else revision


def build_provenance(config: AppConfig, topic_name: str) -> tuple[str, str, str, str]:
    """Build the deterministic configuration and runtime provenance for one topic."""
    relevant_config = {
        "media_cloud": config.media_cloud.model_dump(mode="json"),
        "topic": config.topics[topic_name].model_dump(mode="json"),
    }
    config_json = canonical_json(relevant_config)
    versions_json = canonical_json(
        {
            "python": python_platform.python_version(),
            "media-cloud-pipeline": package_version("media-cloud-pipeline"),
            "mediacloud": package_version("mediacloud"),
        }
    )
    return sha256_hex(relevant_config), config_json, git_revision(), versions_json
