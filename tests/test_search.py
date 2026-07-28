from __future__ import annotations

import csv
from datetime import UTC, date, datetime
from typing import Any

from pydantic import SecretStr
from requests import ConnectionError as RequestsConnectionError

from mc_pipeline.config import AppConfig, load_config
from mc_pipeline.db import init_db
from mc_pipeline.search import (
    estimate_topic,
    export_search_review,
    search_topic,
    split_date_windows,
)


def configured_for_test(
    *,
    start: date = date(2021, 1, 1),
    end: date = date(2021, 1, 2),
    window_days: int = 1,
    page_size: int = 2,
    max_pages: int = 4,
    max_retries: int = 0,
) -> AppConfig:
    config = load_config()
    topic = config.topics["revolving_door_ca"].model_copy(
        update={"start_date": start, "end_date": end}
    )
    media_cloud = config.media_cloud.model_copy(
        update={
            "window_days": window_days,
            "page_size": page_size,
            "max_pages_per_window": max_pages,
            "max_retries": max_retries,
        }
    )
    return config.model_copy(
        update={"media_cloud": media_cloud, "topics": {"revolving_door_ca": topic}}
    )


def story(story_id: str, day: int) -> dict[str, Any]:
    return {
        "id": story_id,
        "indexed_date": datetime(2021, 1, day, 12, tzinfo=UTC),
        "language": "en",
        "media_name": "Example News",
        "media_url": "example.test",
        "publish_date": date(2021, 1, day),
        "title": f"Story {story_id}",
        "url": f"https://www.example.test/{story_id}",
    }


class FakeClient:
    TIMEOUT_SECS = 0.0

    def __init__(
        self,
        *,
        counts: list[dict[str, int]] | None = None,
        pages: list[tuple[list[dict[str, Any]], str | None]] | None = None,
        failures: list[BaseException] | None = None,
    ) -> None:
        self.counts = list(counts or [])
        self.pages = list(pages or [])
        self.failures = list(failures or [])
        self.count_calls = 0
        self.list_calls = 0
        self.expanded_values: list[bool] = []
        self.tokens: list[str | None] = []

    def story_count(self, *args, **kwargs):
        self.count_calls += 1
        if self.failures:
            raise self.failures.pop(0)
        return self.counts.pop(0)

    def story_list(self, *args, **kwargs):
        self.list_calls += 1
        self.expanded_values.append(kwargs["expanded"])
        self.tokens.append(kwargs["pagination_token"])
        if self.failures:
            raise self.failures.pop(0)
        return self.pages.pop(0)


def factory_for(client: FakeClient):
    def factory(token: str) -> FakeClient:
        assert token == "test-token"
        return client

    return factory


def test_split_date_windows_are_closed_non_overlapping_and_include_leap_day():
    windows = split_date_windows(date(2020, 2, 28), date(2020, 3, 2), 2)

    assert windows == [
        (date(2020, 2, 28), date(2020, 2, 29)),
        (date(2020, 3, 1), date(2020, 3, 2)),
    ]


def test_estimate_uses_count_only_persists_results_and_reuses_cache(tmp_path):
    config = configured_for_test()
    client = FakeClient(counts=[{"relevant": 3, "total": 100}, {"relevant": 4, "total": 101}])
    connection = init_db(tmp_path / "estimate.db")
    try:
        first = estimate_topic(
            config,
            "revolving_door_ca",
            connection,
            api_token=SecretStr("test-token"),
            _client_factory=factory_for(client),
        )
        second = estimate_topic(
            config,
            "revolving_door_ca",
            connection,
            api_token=SecretStr("test-token"),
            _client_factory=factory_for(client),
        )

        assert first.succeeded == 2
        assert second.skipped == 2
        assert client.count_calls == 2
        assert client.list_calls == 0
        assert client.TIMEOUT_SECS == 300
        rows = connection.execute(
            "SELECT relevant_count, total_count FROM search_windows ORDER BY window_start"
        ).fetchall()
        assert [tuple(row) for row in rows] == [(3, 100), (4, 101)]
    finally:
        connection.close()


def test_search_persists_pages_raw_payload_and_completed_cache(tmp_path):
    config = configured_for_test(end=date(2021, 1, 1))
    client = FakeClient(
        pages=[([story("one", 1), story("two", 1)], "next"), ([story("three", 1)], None)]
    )
    connection = init_db(tmp_path / "search.db")
    try:
        first = search_topic(
            config,
            "revolving_door_ca",
            connection,
            api_token=SecretStr("test-token"),
            _client_factory=factory_for(client),
        )
        second = search_topic(
            config,
            "revolving_door_ca",
            connection,
            limit_windows=1,
            api_token=SecretStr("test-token"),
            _client_factory=factory_for(client),
        )

        assert first.succeeded == 1
        assert second.skipped == 1
        assert client.list_calls == 2
        assert client.expanded_values == [False, False]
        assert client.tokens == [None, "next"]
        window = connection.execute("SELECT * FROM search_windows").fetchone()
        assert window["pagination_completed"] == 1
        assert window["pages_fetched"] == 2
        assert window["next_pagination_token"] is None
        assert connection.execute("SELECT COUNT(*) FROM stories").fetchone()[0] == 3
        assert connection.execute("SELECT COUNT(*) FROM search_hits").fetchone()[0] == 3
        stored = connection.execute(
            "SELECT domain, raw_json FROM stories WHERE story_id = 'one'"
        ).fetchone()
        assert stored["domain"] == "www.example.test"
        assert set(__import__("json").loads(stored["raw_json"])) == set(story("one", 1))
    finally:
        connection.close()


def test_search_fails_once_continues_and_skips_failed_window_on_rerun(tmp_path):
    config = configured_for_test()
    client = FakeClient(
        pages=[([story("one", 1)], None)],
        failures=[RequestsConnectionError("temporary")],
    )
    connection = init_db(tmp_path / "failed-window.db")
    try:
        first = search_topic(
            config,
            "revolving_door_ca",
            connection,
            api_token=SecretStr("test-token"),
            _client_factory=factory_for(client),
        )
        second = search_topic(
            config,
            "revolving_door_ca",
            connection,
            api_token=SecretStr("test-token"),
            _client_factory=factory_for(client),
        )

        assert first.succeeded == 1
        assert first.failed == 1
        assert client.list_calls == 2
        rows = connection.execute(
            "SELECT status, pagination_completed FROM search_windows ORDER BY window_start"
        ).fetchall()
        assert [tuple(row) for row in rows] == [("failed", 0), ("completed", 1)]
        assert second.skipped == 2
        assert client.list_calls == 2
    finally:
        connection.close()


def test_page_limit_preserves_resume_token_and_marks_failure(tmp_path):
    config = configured_for_test(end=date(2021, 1, 1), max_pages=1)
    client = FakeClient(pages=[([story("one", 1)], "resume-token")])
    connection = init_db(tmp_path / "page-limit.db")
    try:
        summary = search_topic(
            config,
            "revolving_door_ca",
            connection,
            api_token=SecretStr("test-token"),
            _client_factory=factory_for(client),
        )

        window = connection.execute("SELECT * FROM search_windows").fetchone()
        assert summary.failed == 1
        assert window["status"] == "page_limit"
        assert window["pagination_completed"] == 0
        assert window["next_pagination_token"] == "resume-token"
    finally:
        connection.close()


def test_review_export_is_deterministic_and_reports_duplicate_hits(tmp_path):
    config = configured_for_test()
    client = FakeClient(pages=[([story("same", 1)], None), ([story("same", 1)], None)])
    connection = init_db(tmp_path / "review.db")
    output = tmp_path / "review.csv"
    try:
        search_topic(
            config,
            "revolving_door_ca",
            connection,
            api_token=SecretStr("test-token"),
            _client_factory=factory_for(client),
        )
        summary = export_search_review(config, "revolving_door_ca", connection, output=output)

        assert summary.windows == 2
        assert summary.completed_windows == 2
        assert summary.hits == 2
        assert summary.distinct_stories == 1
        assert summary.duplicate_hits == 1
        with output.open(encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        assert [row["window_start"] for row in rows] == ["2021-01-01", "2021-01-02"]
        assert all(row["story_id"] == "same" for row in rows)
    finally:
        connection.close()
