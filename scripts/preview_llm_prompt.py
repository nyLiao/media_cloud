#!/usr/bin/env python
"""Print the configured minimal extraction prompt and strict JSON schema."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mc_pipeline.config import load_config
from mc_pipeline.contracts import build_response_json_schema
from mc_pipeline.prompt import build_extraction_prompt


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, default=Path("config/topics.yaml"))
    parser.add_argument("--topic", default="revolving_door_ca")
    args = parser.parse_args()

    config = load_config(args.config)
    topic = config.topics[args.topic]
    print(build_extraction_prompt(topic.extraction))
    print(json.dumps(build_response_json_schema(topic.extraction), indent=2))


if __name__ == "__main__":
    main()
