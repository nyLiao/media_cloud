#!/usr/bin/env bash
set -euo pipefail

if (($# == 0)); then
  echo "Usage: $0 --topic TOPIC --limit N [mc-pipeline fetch options]" >&2
  exit 64
fi

has_topic=false
has_limit=false
for argument in "$@"; do
  case "$argument" in
    --topic|--topic=*) has_topic=true ;;
    --limit|--limit=*) has_limit=true ;;
  esac
done

if [[ "$has_topic" != true || "$has_limit" != true ]]; then
  echo "Error: Stage 3 launcher requires both --topic and --limit." >&2
  exit 64
fi

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

uv_bin="${UV:-uv}"
if ! command -v "$uv_bin" >/dev/null 2>&1; then
  echo "Error: uv is unavailable. Install uv first." >&2
  exit 127
fi

if ! "$uv_bin" run mc-pipeline fetch --help >/dev/null 2>&1; then
  echo "Error: 'mc-pipeline fetch' is unavailable in this installation." >&2
  exit 127
fi

mkdir -p data/logs
log_file="data/logs/article-fetch-$(date +%Y%m%d-%H%M%S).log"

{
  echo "Logging Stage 3 article fetch to $log_file"
  "$uv_bin" run mc-pipeline fetch --no-progress "$@"
} 2>&1 | tee "$log_file"
