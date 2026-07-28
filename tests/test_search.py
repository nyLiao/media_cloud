from __future__ import annotations

import csv
import json
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


class FatalEstimateError(BaseException):
    pass


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


def test_dry_runs_include_request_parameters_without_provider_calls(tmp_path):
    config = configured_for_test(end=date(2021, 1, 1))
    client = FakeClient()
    connection = init_db(tmp_path / "dry-run.db")
    try:
        estimate = estimate_topic(config, "revolving_door_ca", connection, dry_run=True)
        search = search_topic(config, "revolving_door_ca", connection, dry_run=True)

        assert '"endpoint":"story_count"' in str(estimate)
        assert '"query":' in str(estimate)
        assert '"start_date":"2021-01-01"' in str(estimate)
        assert '"endpoint":"story_list"' in str(search)
        assert '"expanded":false' in str(search)
        assert '"page_size":2' in str(search)
        assert estimate.processed == estimate.succeeded + estimate.skipped + estimate.failed
        assert search.processed == search.succeeded + search.skipped + search.failed
        assert client.count_calls == 0
        assert client.list_calls == 0
    finally:
        connection.close()


def test_estimate_marks_run_failed_on_base_exception(tmp_path):
    config = configured_for_test(end=date(2021, 1, 1))
    client = FakeClient(failures=[FatalEstimateError("interrupted")])
    connection = init_db(tmp_path / "estimate-fatal.db")
    try:
        try:
            estimate_topic(
                config,
                "revolving_door_ca",
                connection,
                api_token=SecretStr("test-token"),
                _client_factory=factory_for(client),
            )
        except FatalEstimateError:
            pass
        else:
            raise AssertionError("FatalEstimateError was not re-raised")

        run = connection.execute(
            "SELECT status, error, completed_at IS NOT NULL FROM pipeline_runs"
        ).fetchone()
        assert tuple(run) == ("failed", "interrupted", 1)
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


def test_search_fails_once_continues_and_retries_failed_window_on_rerun(tmp_path):
    config = configured_for_test()
    client = FakeClient(
        pages=[([story("second-window", 2)], None), ([story("retried", 1)], None)],
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
        assert second.succeeded == 1
        assert second.skipped == 1
        assert client.list_calls == 3
        rows = connection.execute(
            "SELECT status, pagination_completed FROM search_windows ORDER BY window_start"
        ).fetchall()
        assert [tuple(row) for row in rows] == [("completed", 1), ("completed", 1)]
    finally:
        connection.close()


def test_page_limit_preserves_token_and_resumes_after_limit_increases(tmp_path):
    config = configured_for_test(end=date(2021, 1, 1), max_pages=1)
    client = FakeClient(pages=[([story("one", 1)], "resume-token"), ([story("two", 1)], None)])
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

        resumed = search_topic(
            configured_for_test(end=date(2021, 1, 1), max_pages=2),
            "revolving_door_ca",
            connection,
            api_token=SecretStr("test-token"),
            _client_factory=factory_for(client),
        )

        window = connection.execute("SELECT * FROM search_windows").fetchone()
        assert resumed.succeeded == 1
        assert client.tokens == [None, "resume-token"]
        assert window["status"] == "completed"
        assert window["pages_fetched"] == 2
        assert connection.execute("SELECT COUNT(*) FROM stories").fetchone()[0] == 2
    finally:
        connection.close()


def test_malformed_story_is_recorded_without_failing_valid_page_rows(tmp_path):
    config = configured_for_test(end=date(2021, 1, 1))
    malformed = story("ignored", 1)
    malformed["id"] = 42
    malformed["url"] = "https://[invalid/story"
    invalid_url = story("bad-url", 1)
    invalid_url["url"] = "https://[invalid/story"
    client = FakeClient(pages=[([story("valid", 1), malformed, invalid_url], None)])
    connection = init_db(tmp_path / "malformed-story.db")
    try:
        summary = search_topic(
            config,
            "revolving_door_ca",
            connection,
            api_token=SecretStr("test-token"),
            _client_factory=factory_for(client),
        )

        assert (summary.succeeded, summary.failed) == (1, 0)
        assert connection.execute("SELECT COUNT(*) FROM stories").fetchone()[0] == 2
        bad_url_domain = connection.execute(
            "SELECT domain FROM stories WHERE story_id = 'bad-url'"
        ).fetchone()[0]
        assert bad_url_domain is None
        window = connection.execute("SELECT status, raw_json FROM search_windows").fetchone()
        assert window["status"] == "completed"
        page = json.loads(window["raw_json"])["pages"][0]
        assert page["persisted"] == 2
        assert page["record_errors"][0]["result_rank"] == 2
        assert "usable string id" in page["record_errors"][0]["error"]
    finally:
        connection.close()


def test_unsafe_resume_state_is_failed_without_another_provider_call(tmp_path):
    config = configured_for_test(end=date(2021, 1, 1), max_pages=1)
    first_client = FakeClient(pages=[([story("one", 1)], "resume-token")])
    connection = init_db(tmp_path / "unsafe-resume.db")
    try:
        search_topic(
            config,
            "revolving_door_ca",
            connection,
            api_token=SecretStr("test-token"),
            _client_factory=factory_for(first_client),
        )
        connection.execute(
            """
            UPDATE search_windows
            SET status = 'running', next_pagination_token = NULL, error = NULL
            """
        )
        connection.commit()
        second_client = FakeClient(pages=[([story("replayed", 1)], None)])

        summary = search_topic(
            config,
            "revolving_door_ca",
            connection,
            api_token=SecretStr("test-token"),
            _client_factory=factory_for(second_client),
        )

        window = connection.execute("SELECT status, error FROM search_windows").fetchone()
        assert summary.failed == 1
        assert second_client.list_calls == 0
        assert window["status"] == "failed"
        assert "no pagination token" in window["error"]
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
