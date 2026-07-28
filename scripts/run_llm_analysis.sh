#!/usr/bin/env bash
set -euo pipefail

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

mkdir -p data/logs
log_file="data/logs/llm-analysis-$(date +%Y%m%d-%H%M%S).log"

{
  echo "Logging LLM analysis to $log_file"
  "$uv_bin" run mc-pipeline extract "$@"
} 2>&1 | tee "$log_file"
