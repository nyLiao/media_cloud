#!/usr/bin/env bash
set -euo pipefail

if (($# == 0)); then
  echo "Usage: $0 --topic TOPIC --limit N [mc-pipeline extract options]" >&2
  exit 64
fi

has_topic=false
has_limit=false
has_progress_override=false
for argument in "$@"; do
  case "$argument" in
    --topic|--topic=*) has_topic=true ;;
    --limit|--limit=*) has_limit=true ;;
    --progress|--no-progress) has_progress_override=true ;;
  esac
done

if [[ "$has_topic" != true || "$has_limit" != true ]]; then
  echo "Error: Stage 4 launcher requires both --topic and --limit." >&2
  exit 64
fi

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

uv_bin="${UV:-uv}"
if ! command -v "$uv_bin" >/dev/null 2>&1; then
  echo "Error: uv is unavailable. Install uv first." >&2
  exit 127
fi

if ! "$uv_bin" run mc-pipeline extract --help >/dev/null 2>&1; then
  echo "Error: 'mc-pipeline extract' is unavailable in this installation." >&2
  echo "Install a version of media-cloud-pipeline that provides the extract command." >&2
  exit 127
fi

if ! "$uv_bin" run mc-pipeline export --help >/dev/null 2>&1; then
  echo "Error: 'mc-pipeline export' is unavailable in this installation." >&2
  exit 127
fi

mkdir -p data/logs
log_file="data/logs/llm-analysis-$(date +%Y%m%d-%H%M%S).log"
extract_args=("$@")
if [[ "$has_progress_override" != true ]]; then
  extract_args+=(--progress)
fi

{
  echo "Logging LLM analysis to $log_file"
  "$uv_bin" run mc-pipeline extract "${extract_args[@]}"
} 2>&1 | tee "$log_file"
