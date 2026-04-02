#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-python3}"

export PYTHONPATH="$ROOT_DIR${PYTHONPATH:+:$PYTHONPATH}"

# 1) Environment
# This pipeline is stdlib-only, so we avoid requiring venv/pip.
# Optional dependency: requests (only used if UDISE_CSV_ZIP_URL is set).

# 2) Run pipeline stages
"$PYTHON_BIN" scripts/acquire_data.py
"$PYTHON_BIN" scripts/schema_discovery.py
"$PYTHON_BIN" scripts/clean_preprocess.py
"$PYTHON_BIN" scripts/feature_extract.py
"$PYTHON_BIN" scripts/analyze.py

# 3) Contract check
"$PYTHON_BIN" scripts/contract_check.py

echo "OK: outputs written to out/"
ls -1 out | sed 's/^/ - /'
