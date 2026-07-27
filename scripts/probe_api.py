#!/usr/bin/env python
"""Run one bounded Media Cloud diagnostic operation."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from importlib.metadata import version
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from mediacloud.api import DirectoryApi, SearchApi


def parse_date(value: str) -> dt.date:
    return dt.date.fromisoformat(value)


def json_default(value: Any) -> str:
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    return str(value)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("collections", "count", "list"))
    parser.add_argument("--env-file", type=Path, default=Path("config/.env"))
    parser.add_argument("--api-key-env", default="MC_API_TOKEN")
    parser.add_argument("--query")
    parser.add_argument("--collection-id", type=int, action="append", default=[])
    parser.add_argument("--collection-name", default="Canada")
    parser.add_argument("--start-date", type=parse_date)
    parser.add_argument("--end-date", type=parse_date)
    parser.add_argument("--platform", default="onlinenews-mediacloud")
    parser.add_argument("--page-size", type=int, default=3)
    parser.add_argument("--expanded", action="store_true")
    args = parser.parse_args()

    load_dotenv(args.env_file)
    api_key = os.environ.get(args.api_key_env)
    if not api_key:
        raise SystemExit(f"Missing API key env var: {args.api_key_env}")

    if args.mode == "collections":
        result = DirectoryApi(api_key).collection_list(name=args.collection_name, limit=50)
        payload = {
            "operation": "collection_list",
            "mediacloud_version": version("mediacloud"),
            "name": args.collection_name,
            "count": result.get("count"),
            "results": [
                {
                    key: collection.get(key)
                    for key in ("id", "name", "platform", "public", "monitored")
                }
                for collection in result.get("results", [])
            ],
        }
        print(json.dumps(payload, indent=2, default=json_default))
        return

    if not args.query or not args.start_date or not args.end_date:
        parser.error("count/list require --query, --start-date, and --end-date")
    if not args.collection_id:
        parser.error("count/list require at least one --collection-id")

    search = SearchApi(api_key)
    if args.mode == "count":
        result = search.story_count(
            args.query,
            args.start_date,
            args.end_date,
            collection_ids=args.collection_id,
            platform=args.platform,
        )
        payload = {
            "operation": "story_count",
            "mediacloud_version": version("mediacloud"),
            "start_date": args.start_date,
            "end_date": args.end_date,
            "collection_ids": args.collection_id,
            "result": result,
        }
        print(json.dumps(payload, indent=2, default=json_default))
        return

    if not 1 <= args.page_size <= 10:
        parser.error("probe --page-size must be between 1 and 10")
    stories, token = search.story_list(
        args.query,
        args.start_date,
        args.end_date,
        collection_ids=args.collection_id,
        platform=args.platform,
        expanded=args.expanded,
        page_size=args.page_size,
    )
    payload = {
        "operation": "story_list",
        "mediacloud_version": version("mediacloud"),
        "start_date": args.start_date,
        "end_date": args.end_date,
        "collection_ids": args.collection_id,
        "expanded": args.expanded,
        "returned": len(stories),
        "has_pagination_token": bool(token),
        "stories": [
            {
                "keys": sorted(story),
                "id": story.get("id"),
                "title": str(story.get("title", ""))[:240],
                "url": str(story.get("url", ""))[:500],
                "publish_date": story.get("publish_date"),
                "media_name": story.get("media_name"),
                "text_chars": len(story["text"]) if isinstance(story.get("text"), str) else None,
            }
            for story in stories
        ],
    }
    print(json.dumps(payload, indent=2, default=json_default))


if __name__ == "__main__":
    main()
