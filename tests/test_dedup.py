from __future__ import annotations

import csv
from datetime import date

from mc_pipeline.config import AppConfig, load_config
from mc_pipeline.db import init_db
from mc_pipeline.dedup import (
    _canonical,
    deduplicate_topic,
    export_dedup_review,
    normalize_title,
    normalize_url,
)


def configured_for_dedup(*, allowlist: list[str] | None = None) -> AppConfig:
    config = load_config()
    topic = config.topics["revolving_door_ca"].model_copy(
        update={
            "start_date": date(2021, 1, 1),
            "end_date": date(2021, 12, 31),
            "domain_allowlist": [] if allowlist is None else allowlist,
        }
    )
    return config.model_copy(update={"topics": {"revolving_door_ca": topic}})


def seed_topic_stories(connection, topic: str, stories: list[dict[str, str | None]]) -> int:
    window_id = connection.execute(
        """
        INSERT INTO search_windows(
            topic, query_hash, request_fingerprint, query, platform, collection_ids_json,
            source_ids_json, languages_json, window_start, window_end, mediacloud_version
        )
        VALUES (?, 'query', ?, 'query', 'online_news', '[1]', '[]', '["en"]',
                '2021-01-01', '2021-12-31', 'test')
        """,
        (topic, f"fingerprint-{topic}"),
    ).lastrowid
    assert window_id is not None
    for rank, story in enumerate(stories, start=1):
        connection.execute(
            """
            INSERT INTO stories(story_id, title, url, domain, publish_date, language)
            VALUES (:story_id, :title, :url, :domain, :publish_date, :language)
            ON CONFLICT(story_id) DO NOTHING
            """,
            story,
        )
        connection.execute(
            """
            INSERT INTO search_hits(search_window_id, story_id, page_number, result_rank)
            VALUES (?, ?, 1, ?)
            """,
            (window_id, story["story_id"], rank),
        )
    connection.commit()
    return window_id


def story(
    story_id: str,
    *,
    title: str = "A sufficiently long policy headline",
    url: str | None = None,
    publish_date: str = "2021-06-01",
    language: str = "en",
) -> dict[str, str | None]:
    return {
        "story_id": story_id,
        "title": title,
        "url": url if url is not None else f"https://www.example.test/{story_id}",
        "domain": "www.example.test",
        "publish_date": publish_date,
        "language": language,
    }


def state_rows(connection, topic: str):
    return connection.execute(
        """
        SELECT story_id, qc_status, qc_reason, dup_of_story_id, decided_at
        FROM story_topic_state
        WHERE topic = ?
        ORDER BY story_id
        """,
        (topic,),
    ).fetchall()


def test_normalizers_remove_only_tracking_and_outlet_suffixes():
    assert (
        normalize_url("HTTPS://www.Example.test/path/?b=2&utm_source=newsletter&a=1#section")
        == "example.test/path?a=1&b=2"
    )
    assert normalize_title("Policy - impacts", "Example News") == "policy impacts"
    assert normalize_title("Policy - The Globe and Mail") == "policy"
    assert normalize_title("Policy | Custom Outlet", "Custom Outlet") == "policy"
    assert normalize_url("https://[invalid/story") is None


def test_canonical_falls_back_to_story_id_when_publish_date_is_missing():
    first = {
        "story_id": "a",
        "url_norm": "example.test/a",
        "title_norm": "same",
        "qc_status": "ok",
        "qc_reason": None,
        "dup_of_story_id": None,
    }
    second = {**first, "story_id": "b"}

    assert _canonical([second, first], {})["story_id"] == "a"


def test_deduplicate_applies_qc_precedence_exact_dedup_and_provenance(tmp_path):
    connection = init_db(tmp_path / "dedup.db")
    try:
        config = configured_for_dedup(allowlist=["example.test"])
        missing_url = story("bad-url")
        missing_url["url"] = None
        missing_url["domain"] = None
        seed_topic_stories(
            connection,
            "revolving_door_ca",
            [
                story("bad-lang", language="fr", publish_date="2020-01-01", url="not a url"),
                story("out-of-range", publish_date="2022-01-01"),
                missing_url,
                story("short-title", title="Short"),
                story("off-domain", url="https://outside.test/article"),
                story(
                    "url-earlier",
                    title="First long duplicate headline",
                    url="https://www.example.test/path/?utm_source=newsletter&fbclid=abc",
                    publish_date="2021-02-01",
                ),
                story(
                    "url-later",
                    title="Second long duplicate headline",
                    url="http://example.test/path",
                    publish_date="2021-03-01",
                ),
                story(
                    "a-title",
                    title="Government policy changes - The Globe and Mail",
                    publish_date="2021-04-01",
                ),
                story(
                    "z-title",
                    title="Government policy changes | CBC News",
                    publish_date="2021-04-01",
                ),
            ],
        )

        summary = deduplicate_topic(config, "revolving_door_ca", connection)

        assert (
            summary.processed,
            summary.succeeded,
            summary.skipped,
            summary.failed,
        ) == (9, 2, 7, 0)
        assert [tuple(row[:4]) for row in state_rows(connection, "revolving_door_ca")] == [
            ("a-title", "ok", None, None),
            ("bad-lang", "bad_lang", "bad_lang", None),
            ("bad-url", "bad_url", "bad_url", None),
            ("off-domain", "off_domain", "off_domain", None),
            ("out-of-range", "out_of_range", "out_of_range", None),
            ("short-title", "short_title", "short_title", None),
            ("url-earlier", "ok", None, None),
            ("url-later", "dup", "duplicate_url", "url-earlier"),
            ("z-title", "dup", "duplicate_title", "a-title"),
        ]
        norms = connection.execute(
            "SELECT url_norm, title_norm FROM stories WHERE story_id = 'url-earlier'"
        ).fetchone()
        assert tuple(norms) == ("example.test/path", "first long duplicate headline")
        run = connection.execute(
            "SELECT stage, status FROM pipeline_runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert tuple(run) == ("dedup", "completed")
    finally:
        connection.close()


def test_dedup_review_export_is_deterministic_and_reconciles_outcomes(tmp_path):
    connection = init_db(tmp_path / "dedup-review.db")
    output = tmp_path / "dedup-review.csv"
    try:
        config = configured_for_dedup()
        seed_topic_stories(
            connection,
            "revolving_door_ca",
            [
                story("z-duplicate", url="https://example.test/shared", publish_date="2021-02-01"),
                story("a-canonical", url="https://example.test/shared", publish_date="2021-01-01"),
                story("bad-language", language="fr"),
            ],
        )

        deduplicate_topic(config, "revolving_door_ca", connection)
        summary = export_dedup_review(config, "revolving_door_ca", connection, output=output)

        assert (
            summary.stories,
            summary.decided,
            summary.accepted,
            summary.duplicates,
            summary.rejected,
            summary.undecided,
            summary.duplicate_urls,
            summary.duplicate_titles,
        ) == (3, 3, 1, 1, 1, 0, 1, 0)
        with output.open(encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        assert [row["story_id"] for row in rows] == [
            "a-canonical",
            "bad-language",
            "z-duplicate",
        ]
        assert rows[2]["qc_reason"] == "duplicate_url"
        assert rows[2]["dup_of_story_id"] == "a-canonical"
        assert rows[2]["duplicate_of_url"] == "https://example.test/shared"
    finally:
        connection.close()


def test_deduplicate_preserves_unchanged_decisions_and_removes_stale_state(tmp_path):
    connection = init_db(tmp_path / "rerun.db")
    try:
        config = configured_for_dedup()
        window_id = seed_topic_stories(
            connection,
            "revolving_door_ca",
            [story("keep"), story("stale")],
        )
        deduplicate_topic(config, "revolving_door_ca", connection)
        connection.execute(
            """
            UPDATE story_topic_state
            SET decided_at = 'unchanged'
            WHERE topic = ? AND story_id = ?
            """,
            ("revolving_door_ca", "keep"),
        )
        connection.execute(
            "DELETE FROM search_hits WHERE search_window_id = ? AND story_id = ?",
            (window_id, "stale"),
        )
        connection.commit()

        summary = deduplicate_topic(config, "revolving_door_ca", connection)

        assert (
            summary.processed,
            summary.succeeded,
            summary.skipped,
            summary.failed,
        ) == (1, 1, 0, 0)
        assert [tuple(row[:4]) for row in state_rows(connection, "revolving_door_ca")] == [
            ("keep", "ok", None, None)
        ]
        assert state_rows(connection, "revolving_door_ca")[0]["decided_at"] == "unchanged"
    finally:
        connection.close()


def test_qc_rejection_cannot_become_duplicate_canonical(tmp_path):
    connection = init_db(tmp_path / "qc-first.db")
    try:
        config = configured_for_dedup()
        seed_topic_stories(
            connection,
            "revolving_door_ca",
            [
                story(
                    "sports-earlier",
                    title="Shared sufficiently long headline",
                    url="https://example.test/sports/story",
                    publish_date="2021-01-01",
                ),
                story(
                    "valid-later",
                    title="Shared sufficiently long headline",
                    url="https://example.test/news/story",
                    publish_date="2021-02-01",
                ),
            ],
        )

        deduplicate_topic(config, "revolving_door_ca", connection)

        assert [tuple(row[:4]) for row in state_rows(connection, "revolving_door_ca")] == [
            ("sports-earlier", "bad_url", "bad_url", None),
            ("valid-later", "ok", None, None),
        ]
    finally:
        connection.close()


def test_sequential_duplicate_rules_point_to_final_canonical(tmp_path):
    connection = init_db(tmp_path / "canonical-chain.db")
    try:
        config = configured_for_dedup()
        seed_topic_stories(
            connection,
            "revolving_door_ca",
            [
                story(
                    "url-canonical",
                    title="Shared sufficiently long headline",
                    url="https://example.test/shared-url",
                    publish_date="2021-02-01",
                ),
                story(
                    "url-copy",
                    title="Different sufficiently long headline",
                    url="http://www.example.test/shared-url",
                    publish_date="2021-03-01",
                ),
                story(
                    "title-canonical",
                    title="Shared sufficiently long headline",
                    url="https://example.test/other-url",
                    publish_date="2021-01-01",
                ),
            ],
        )

        deduplicate_topic(config, "revolving_door_ca", connection)

        assert [tuple(row[:4]) for row in state_rows(connection, "revolving_door_ca")] == [
            ("title-canonical", "ok", None, None),
            ("url-canonical", "dup", "duplicate_title", "title-canonical"),
            ("url-copy", "dup", "duplicate_url", "title-canonical"),
        ]
    finally:
        connection.close()


def test_changed_qc_config_recomputes_existing_decision(tmp_path):
    connection = init_db(tmp_path / "config-change.db")
    try:
        restricted = configured_for_dedup(allowlist=["allowed.test"])
        seed_topic_stories(
            connection,
            "revolving_door_ca",
            [story("recomputed", url="https://example.test/article")],
        )
        deduplicate_topic(restricted, "revolving_door_ca", connection)
        assert tuple(state_rows(connection, "revolving_door_ca")[0][1:4]) == (
            "off_domain",
            "off_domain",
            None,
        )

        unrestricted = configured_for_dedup()
        deduplicate_topic(unrestricted, "revolving_door_ca", connection)

        assert tuple(state_rows(connection, "revolving_door_ca")[0][1:4]) == (
            "ok",
            None,
            None,
        )
    finally:
        connection.close()


def test_deduplicate_keeps_topic_decisions_isolated(tmp_path):
    connection = init_db(tmp_path / "topics.db")
    try:
        config = configured_for_dedup()
        english = config.topics["revolving_door_ca"]
        french = english.model_copy(update={"languages": ["fr"]})
        config = config.model_copy(update={"topics": {"english": english, "french": french}})
        shared = story("shared", language="fr")
        seed_topic_stories(connection, "english", [shared])
        seed_topic_stories(connection, "french", [shared])

        english_summary = deduplicate_topic(config, "english", connection)
        french_summary = deduplicate_topic(config, "french", connection)

        assert (english_summary.succeeded, english_summary.skipped) == (0, 1)
        assert (french_summary.succeeded, french_summary.skipped) == (1, 0)
        rows = connection.execute(
            """
            SELECT topic, qc_status, qc_reason
            FROM story_topic_state
            WHERE story_id = 'shared'
            ORDER BY topic
            """
        ).fetchall()
        assert [tuple(row) for row in rows] == [
            ("english", "bad_lang", "bad_lang"),
            ("french", "ok", None),
        ]
    finally:
        connection.close()
