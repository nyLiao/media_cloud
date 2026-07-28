from __future__ import annotations

import csv
import gzip
import sqlite3
from collections import deque
from pathlib import Path
from typing import Any

import pytest
import yaml
from requests import ConnectionError as RequestsConnectionError

from mc_pipeline.cli import _RichFetchProgressReporter
from mc_pipeline.config import AppConfig
from mc_pipeline.db import init_db
from mc_pipeline.errors import ConfigError
from mc_pipeline.fetch import FETCH_REVIEW_COLUMNS, FetchProgress, export_fetch_review, fetch_topic


class FakeResponse:
    def __init__(
        self,
        *,
        status_code: int = 200,
        body: bytes = b"",
        content_type: str = "text/html; charset=utf-8",
        url: str = "https://example.test/article",
    ) -> None:
        self.status_code = status_code
        self._body = body
        self.headers = {"Content-Type": content_type}
        self.url = url
        self.encoding = "utf-8"

    def iter_content(self, chunk_size: int):
        del chunk_size
        yield self._body

    def close(self) -> None:
        pass


class FakeSession:
    def __init__(self, responses: dict[str, list[FakeResponse | BaseException]]) -> None:
        self.responses = {url: deque(items) for url, items in responses.items()}
        self.calls: list[str] = []
        self.max_redirects = 30

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        assert kwargs["allow_redirects"] is True
        assert kwargs["stream"] is True
        self.calls.append(url)
        result = self.responses[url].popleft()
        if isinstance(result, BaseException):
            raise result
        return result


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def configured_for_test(*, respect_robots: bool = False) -> AppConfig:
    payload = yaml.safe_load(Path("config/topics.yaml").read_text(encoding="utf-8"))
    config = AppConfig.model_validate(payload)
    fetch = config.fetch.model_copy(
        update={
            "min_text_chars": 20,
            "respect_robots": respect_robots,
            "save_raw_html": False,
            "per_domain_delay_s": 2.0,
            "global_max_rps": 10.0,
            "timeout_s": 5.0,
            "user_agent": "media-cloud-research/0.1 (+mailto:researcher@university.edu)",
        }
    )
    return config.model_copy(update={"fetch": fetch})


def seed_story(
    connection: sqlite3.Connection,
    *,
    story_id: str,
    title: str,
    url: str | None,
    mc_text: str | None = None,
    topic: str = "revolving_door_ca",
) -> None:
    existing_window = connection.execute(
        "SELECT id FROM search_windows WHERE topic = ?", (topic,)
    ).fetchone()
    if existing_window is None:
        connection.execute(
            """
            INSERT INTO search_windows(
                topic, query_hash, request_fingerprint, query, platform, collection_ids_json,
                source_ids_json, languages_json, window_start, window_end, mediacloud_version
            )
            VALUES (?, ?, ?, 'query', 'platform', '[1]', '[]', '["en"]', '2021-01-01',
                    '2021-01-01', 'test')
            """,
            (topic, f"query-{topic}", f"window-{topic}"),
        )
        window_id = connection.execute("SELECT last_insert_rowid()").fetchone()[0]
    else:
        window_id = existing_window["id"]
    connection.execute(
        "INSERT INTO stories(story_id, title, url, mc_text) VALUES (?, ?, ?, ?)",
        (story_id, title, url, mc_text),
    )
    connection.execute(
        "INSERT INTO search_hits(search_window_id, story_id) VALUES (?, ?)",
        (window_id, story_id),
    )
    connection.execute(
        "INSERT INTO story_topic_state(topic, story_id, qc_status) VALUES (?, ?, 'ok')",
        (topic, story_id),
    )
    connection.commit()


def seed_fetch_outcome(
    connection: sqlite3.Connection,
    *,
    story_id: str,
    fetched_at: str,
    fetch_status: str = "ok",
    source: str = "url",
    text: str = "Stored extracted article text.",
) -> None:
    connection.execute(
        """
        UPDATE stories
        SET domain = ?, publish_date = ?, media_name = ?
        WHERE story_id = ?
        """,
        ("example.test", "2024-01-01", "Example News", story_id),
    )
    connection.execute(
        """
        INSERT INTO articles(
            story_id, fetch_status, source, http_status, final_url, extractor, text_chars,
            attempts, error, raw_html_path, text, fetched_at
        )
        VALUES (?, ?, ?, 200, ?, 'trafilatura', 321, 1, ?, ?, ?, ?)
        """,
        (
            story_id,
            fetch_status,
            source,
            f"https://example.test/{story_id}/final",
            None if fetch_status == "ok" else "blocked by publisher",
            f"data/raw_html/{story_id}.html.gz",
            text,
            fetched_at,
        ),
    )
    connection.commit()


def test_fetch_dry_run_is_read_only_and_never_uses_session(tmp_path):
    connection = init_db(tmp_path / "fetch-dry-run.db")
    try:
        seed_story(
            connection,
            story_id="story-1",
            title="A story without cached text",
            url="https://example.test/article",
        )
        session = FakeSession({})
        updates: list[FetchProgress] = []

        summary = fetch_topic(
            configured_for_test(),
            "revolving_door_ca",
            connection,
            dry_run=True,
            progress=updates.append,
            _session=session,
        )

        assert summary.processed == summary.skipped == 1
        assert summary.succeeded == summary.failed == 0
        assert '"endpoint":"article_fetch"' in str(summary)
        assert '"source":"url"' in str(summary)
        assert session.calls == []
        assert updates == [FetchProgress(1, 1, "story-1", "unknown", "planned", 0, 0)]
        assert connection.execute("SELECT COUNT(*) FROM articles").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM fetch_attempts").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0] == 0
    finally:
        connection.close()


def test_disabled_progress_still_emits_structured_entry_log(capsys):
    reporter = _RichFetchProgressReporter(enabled=False)

    reporter(FetchProgress(1, 2, "story-1", "example.test", "ok", 1, 0))
    reporter.close()

    captured = capsys.readouterr()
    assert '"event":"fetch_entry"' in captured.err
    assert '"story_id":"story-1"' in captured.err
    assert captured.err.count("\n") == 1


def test_fetch_uses_priority_robots_cache_rate_delay_and_progress(tmp_path, monkeypatch):
    config = configured_for_test(respect_robots=True)
    second_topic = config.topics["revolving_door_ca"].model_copy(update={"label": "Second"})
    config = config.model_copy(
        update={
            "topics": {
                "revolving_door_ca": config.topics["revolving_door_ca"],
                "second": second_topic,
            }
        }
    )
    connection = init_db(tmp_path / "fetch-success.db")
    try:
        seed_story(
            connection,
            story_id="low",
            title="General government story",
            url="https://example.test/low",
        )
        seed_story(
            connection,
            story_id="high",
            title="Lobbying appointment story",
            url="https://example.test/high",
        )
        session = FakeSession(
            {
                "https://example.test/robots.txt": [
                    FakeResponse(body=b"User-agent: *\nAllow: /\n")
                ],
                "https://example.test/high": [FakeResponse(body=b"<p>high article body</p>")],
                "https://example.test/low": [FakeResponse(body=b"<p>low article body</p>")],
            }
        )
        clock = FakeClock()
        updates: list[FetchProgress] = []
        monkeypatch.setattr(
            "mc_pipeline.fetch._extract_text",
            lambda html, minimum_chars: ("extracted article text long enough", "trafilatura"),
        )

        summary = fetch_topic(
            config,
            "revolving_door_ca",
            connection,
            progress=updates.append,
            _session=session,
            _clock=clock,
            _sleeper=clock.sleep,
            _random_uniform=lambda _minimum, _maximum: 2.0,
        )

        assert summary.processed == summary.succeeded == 2
        assert summary.failed == 0
        assert session.calls == [
            "https://example.test/robots.txt",
            "https://example.test/high",
            "https://example.test/low",
        ]
        assert clock.sleeps == [0.1, 1.9, 2.0]
        assert [update.story_id for update in updates] == ["high", "low"]
        assert all(update.status == "ok" for update in updates)
        rows = connection.execute(
            "SELECT story_id, fetch_status, extractor FROM articles ORDER BY story_id"
        ).fetchall()
        assert [tuple(row) for row in rows] == [
            ("high", "ok", "trafilatura"),
            ("low", "ok", "trafilatura"),
        ]
        assert connection.execute("SELECT COUNT(*) FROM fetch_attempts").fetchone()[0] == 3

        connection.execute(
            """
            INSERT INTO search_windows(
                topic, query_hash, request_fingerprint, query, platform, collection_ids_json,
                source_ids_json, languages_json, window_start, window_end, mediacloud_version
            )
            VALUES ('second', 'query-second', 'window-second', 'query', 'platform', '[1]', '[]',
                    '["en"]', '2021-01-01', '2021-01-01', 'test')
            """
        )
        window_id = connection.execute("SELECT last_insert_rowid()").fetchone()[0]
        connection.execute(
            "INSERT INTO search_hits(search_window_id, story_id) VALUES (?, 'high')", (window_id,)
        )
        connection.execute(
            "INSERT INTO story_topic_state(topic, story_id, qc_status) "
            "VALUES ('second', 'high', 'ok')"
        )
        connection.commit()

        reused = fetch_topic(config, "second", connection, _session=session)

        assert reused.processed == 0
        assert session.calls == [
            "https://example.test/robots.txt",
            "https://example.test/high",
            "https://example.test/low",
        ]
    finally:
        connection.close()


def test_fetch_prefers_qualifying_mediacloud_text_without_http(tmp_path):
    connection = init_db(tmp_path / "fetch-mediacloud.db")
    try:
        cached_text = "Media Cloud text that already exceeds the configured minimum."
        seed_story(
            connection,
            story_id="cached",
            title="Cached story",
            url="https://example.test/cached",
            mc_text=cached_text,
        )
        session = FakeSession({})

        summary = fetch_topic(
            configured_for_test(),
            "revolving_door_ca",
            connection,
            _session=session,
        )

        assert summary.processed == summary.succeeded == 1
        assert session.calls == []
        article = connection.execute(
            "SELECT fetch_status, source, extractor, text FROM articles WHERE story_id = 'cached'"
        ).fetchone()
        assert tuple(article) == ("ok", "mediacloud", "mediacloud", cached_text)
        assert connection.execute("SELECT COUNT(*) FROM fetch_attempts").fetchone()[0] == 0
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("response", "expected_status"),
    [
        (FakeResponse(status_code=403), "blocked"),
        (RequestsConnectionError("offline"), "fetch_failed"),
    ],
)
def test_fetch_records_single_terminal_failure(tmp_path, response, expected_status):
    connection = init_db(tmp_path / f"fetch-{expected_status}.db")
    try:
        seed_story(
            connection,
            story_id="failure",
            title="Failure story",
            url="https://example.test/failure",
        )
        session = FakeSession({"https://example.test/failure": [response]})

        summary = fetch_topic(
            configured_for_test(),
            "revolving_door_ca",
            connection,
            _session=session,
        )

        assert summary.processed == summary.failed == 1
        assert summary.succeeded == 0
        assert session.calls == ["https://example.test/failure"]
        article = connection.execute(
            "SELECT fetch_status, attempts FROM articles WHERE story_id = 'failure'"
        ).fetchone()
        assert article["fetch_status"] == expected_status
        assert article["attempts"] == 1
        assert connection.execute("SELECT COUNT(*) FROM fetch_attempts").fetchone()[0] == 1
    finally:
        connection.close()


def test_live_fetch_rejects_placeholder_contact_but_dry_run_allows_it(tmp_path):
    connection = init_db(tmp_path / "fetch-contact.db")
    try:
        seed_story(
            connection,
            story_id="contact",
            title="Contact validation story",
            url="https://example.test/contact",
        )
        payload = yaml.safe_load(Path("config/topics.yaml").read_text(encoding="utf-8"))
        config = AppConfig.model_validate(payload)
        config = config.model_copy(
            update={
                "fetch": config.fetch.model_copy(
                    update={"user_agent": "media-cloud-research/0.1 (+mailto:you@example.org)"}
                )
            }
        )

        dry_run = fetch_topic(config, "revolving_door_ca", connection, dry_run=True)
        assert dry_run.skipped == 1

        with pytest.raises(ConfigError, match="real mailto contact"):
            fetch_topic(config, "revolving_door_ca", connection, _session=FakeSession({}))
        assert connection.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0] == 0
    finally:
        connection.close()


def test_terminal_story_gets_one_new_attempt_on_explicit_new_run(tmp_path, monkeypatch):
    connection = init_db(tmp_path / "fetch-rerun.db")
    try:
        seed_story(
            connection,
            story_id="rerun",
            title="Rerun story",
            url="https://example.test/rerun",
        )
        session = FakeSession(
            {
                "https://example.test/rerun": [
                    FakeResponse(status_code=403),
                    FakeResponse(body=b"<p>article body</p>"),
                ]
            }
        )
        monkeypatch.setattr(
            "mc_pipeline.fetch._extract_text",
            lambda html, minimum_chars: ("extracted article text long enough", "trafilatura"),
        )

        first = fetch_topic(
            configured_for_test(), "revolving_door_ca", connection, _session=session
        )
        second = fetch_topic(
            configured_for_test(), "revolving_door_ca", connection, _session=session
        )

        assert first.failed == 1
        assert second.succeeded == 1
        article = connection.execute(
            "SELECT fetch_status, attempts FROM articles WHERE story_id = 'rerun'"
        ).fetchone()
        assert tuple(article) == ("ok", 2)
        assert connection.execute("SELECT COUNT(*) FROM fetch_attempts").fetchone()[0] == 2
    finally:
        connection.close()


def test_stored_html_reextracts_without_network(tmp_path, monkeypatch):
    connection = init_db(tmp_path / "fetch-reextract.db")
    try:
        seed_story(
            connection,
            story_id="stored",
            title="Stored HTML story",
            url="https://example.test/stored",
        )
        raw_path = tmp_path / "stored.html.gz"
        with gzip.open(raw_path, "wb") as compressed:
            compressed.write(b"<p>stored article body</p>")
        connection.execute(
            """
            INSERT INTO articles(story_id, fetch_status, attempts, raw_html_path)
            VALUES ('stored', 'too_short', 1, ?)
            """,
            (str(raw_path),),
        )
        connection.commit()
        session = FakeSession({})
        monkeypatch.setattr(
            "mc_pipeline.fetch._extract_text",
            lambda html, minimum_chars: ("re-extracted article text", "beautifulsoup"),
        )

        summary = fetch_topic(
            configured_for_test(), "revolving_door_ca", connection, _session=session
        )

        assert summary.succeeded == 1
        assert session.calls == []
        article = connection.execute(
            "SELECT fetch_status, source, attempts FROM articles WHERE story_id = 'stored'"
        ).fetchone()
        assert tuple(article) == ("ok", "stored_html", 1)
        assert connection.execute("SELECT COUNT(*) FROM fetch_attempts").fetchone()[0] == 0
    finally:
        connection.close()


def test_fetch_review_excludes_terminal_rows_before_limit_and_is_read_only(tmp_path):
    connection = init_db(tmp_path / "fetch-review.db")
    output = tmp_path / "review" / "fetch.csv"
    try:
        for story_id in (
            "latest",
            "a-tie",
            "z-tie",
            "old-ok",
            "failed",
            "blocked",
            "terminal",
            "excluded",
        ):
            seed_story(
                connection,
                story_id=story_id,
                title=f"{story_id} title",
                url=f"https://example.test/{story_id}",
            )
        seed_story(
            connection,
            story_id="other-topic",
            title="Other topic title",
            url="https://example.test/other-topic",
            topic="other-topic",
        )

        stored_text = "Full stored text.\n\nIt includes every extracted paragraph."
        seed_fetch_outcome(
            connection,
            story_id="latest",
            fetched_at="2024-03-02T00:00:00Z",
            text=stored_text,
        )
        seed_fetch_outcome(connection, story_id="z-tie", fetched_at="2024-03-01T00:00:00Z")
        seed_fetch_outcome(connection, story_id="a-tie", fetched_at="2024-03-01T00:00:00Z")
        seed_fetch_outcome(connection, story_id="old-ok", fetched_at="2024-01-01T00:00:00Z")
        seed_fetch_outcome(
            connection,
            story_id="failed",
            fetched_at="2024-06-03T00:00:00Z",
            fetch_status="failed",
        )
        seed_fetch_outcome(
            connection,
            story_id="blocked",
            fetched_at="2024-06-02T00:00:00Z",
            fetch_status="blocked",
        )
        seed_fetch_outcome(
            connection,
            story_id="terminal",
            fetched_at="2024-06-01T00:00:00Z",
            fetch_status="too_short",
        )
        seed_fetch_outcome(connection, story_id="excluded", fetched_at="2024-04-01T00:00:00Z")
        seed_fetch_outcome(connection, story_id="other-topic", fetched_at="2024-04-02T00:00:00Z")
        connection.execute(
            "UPDATE story_topic_state SET qc_status = 'dup' WHERE story_id = 'excluded'"
        )
        connection.commit()

        denied_actions = {sqlite3.SQLITE_DELETE, sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE}
        connection.set_authorizer(
            lambda action, _arg1, _arg2, _database, _source: (
                sqlite3.SQLITE_DENY if action in denied_actions else sqlite3.SQLITE_OK
            )
        )
        try:
            summary = export_fetch_review(
                configured_for_test(),
                "revolving_door_ca",
                connection,
                limit=3,
                output=output,
            )
        finally:
            connection.set_authorizer(None)

        assert (
            summary.topic,
            summary.limit,
            summary.exported,
            summary.output_path,
        ) == ("revolving_door_ca", 3, 3, output)
        assert connection.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0] == 0
        with output.open(encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        assert tuple(rows[0]) == FETCH_REVIEW_COLUMNS
        assert [row["story_id"] for row in rows] == ["latest", "a-tie", "z-tie"]
        assert all(row["topic"] == "revolving_door_ca" for row in rows)
        assert all(row["fetch_status"] == "ok" for row in rows)
        assert rows[0]["final_url"] == "https://example.test/latest/final"
        assert rows[0]["text"] == stored_text
        assert rows[0]["text_chars"] == "321"
        assert "raw_html_path" not in rows[0]
    finally:
        connection.close()


@pytest.mark.parametrize("limit", [0, -1, 101])
def test_fetch_review_validates_bounded_limit(tmp_path, limit):
    connection = init_db(tmp_path / "fetch-review-limit.db")
    try:
        with pytest.raises(ConfigError, match="between 1 and 100"):
            export_fetch_review(
                configured_for_test(),
                "revolving_door_ca",
                connection,
                limit=limit,
                output=tmp_path / "fetch.csv",
            )
    finally:
        connection.close()
