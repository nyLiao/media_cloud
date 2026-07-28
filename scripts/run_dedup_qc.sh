#!/usr/bin/env bash
set -euo pipefail

if (($# == 0)); then
  echo "Usage: $0 --topic TOPIC [mc-pipeline dedup options]" >&2
  exit 64
fi

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

uv_bin="${UV:-uv}"
if ! command -v "$uv_bin" >/dev/null 2>&1; then
  echo "Error: uv is unavailable. Install uv first." >&2
  exit 127
fi

mkdir -p data/logs
log_file="data/logs/dedup-qc-$(date +%Y%m%d-%H%M%S).log"

{
  echo "Logging Stage 2 dedup/QC to $log_file"
  "$uv_bin" run mc-pipeline dedup "$@"
} 2>&1 | tee "$log_file"
