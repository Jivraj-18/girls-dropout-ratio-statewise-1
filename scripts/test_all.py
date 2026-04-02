#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT_DIR / "scripts"
OUT_DIR = ROOT_DIR / "out"
DATA_DIR = ROOT_DIR / "data"

STAGE_SCRIPTS = [
    "acquire_data.py",
    "schema_discovery.py",
    "clean_preprocess.py",
    "feature_extract.py",
    "analyze.py",
    "contract_check.py",
]

REQUIRED_OUTPUTS = [
    "schema.json",
    "summary_stats.json",
    "analysis_results.json",
    "feature_importance.json",
    "data_manifest.json",
]


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _run_script(name: str, env: dict[str, str]) -> None:
    script = SCRIPTS_DIR / name
    if not script.exists():
        raise SystemExit(f"Missing script: {script}")

    cmd = [sys.executable, str(script)]
    print(f"==> RUN {' '.join(cmd)}")

    try:
        subprocess.run(cmd, cwd=str(ROOT_DIR), env=env, check=True)
    except subprocess.CalledProcessError as e:
        raise SystemExit(f"Script failed: {name} (exit={e.returncode})") from e


def _clean_local_state() -> None:
    if OUT_DIR.exists():
        shutil.rmtree(OUT_DIR)

    extract_root = DATA_DIR / "extract"
    if extract_root.exists():
        shutil.rmtree(extract_root)


def _base_env() -> dict[str, str]:
    env = dict(os.environ)

    # Ensure scripts can import `pipeline/` when run directly.
    env["PYTHONPATH"] = str(ROOT_DIR)

    # Force "independent" mode: do not point at any external data.
    env.pop("UDISE_DATA_PATH", None)

    # Allow override of the upstream source, but default to script's default repo.
    env.pop("UDISE_CSV_ZIP_URL", None)

    # If caller set a custom repo URL, keep it. Otherwise acquire_data.py uses its own default.
    return env


def _hash_required_outputs() -> dict[str, str]:
    missing = [f for f in REQUIRED_OUTPUTS if not (OUT_DIR / f).exists()]
    if missing:
        raise SystemExit(f"Missing required outputs after run: {missing}")
    return {f: _sha256_file(OUT_DIR / f) for f in REQUIRED_OUTPUTS}


def main() -> None:
    env = _base_env()

    print("Cleaning local state (out/, data/extract/)...")
    _clean_local_state()

    for s in STAGE_SCRIPTS:
        _run_script(s, env)

    hashes_1 = _hash_required_outputs()

    print("Re-running analysis stages to check determinism (no re-download)...")
    # Re-run from schema discovery onwards; keep extracted data.
    for s in STAGE_SCRIPTS[1:]:
        _run_script(s, env)

    hashes_2 = _hash_required_outputs()

    if hashes_1 != hashes_2:
        diffs = [k for k in REQUIRED_OUTPUTS if hashes_1.get(k) != hashes_2.get(k)]
        raise SystemExit(f"Non-deterministic outputs detected for: {diffs}")

    print("OK: all scripts ran successfully and outputs are deterministic")


if __name__ == "__main__":
    main()
