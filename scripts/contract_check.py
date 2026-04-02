from __future__ import annotations

import json
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT_DIR / "out"
DATA_DIR = ROOT_DIR / "data"

REQUIRED = [
    "schema.json",
    "summary_stats.json",
    "analysis_results.json",
    "feature_importance.json",
    "data_manifest.json",
]

FORBIDDEN_LITERALS = ["NaN", "Infinity", "-Infinity"]


def _load_json_strict(path: Path) -> object:
    text = path.read_text(encoding="utf-8")
    for lit in FORBIDDEN_LITERALS:
        if lit in text:
            raise ValueError(f"Forbidden literal {lit} found in {path}")
    return json.loads(text)


def main() -> None:
    missing = [f for f in REQUIRED if not (OUT_DIR / f).exists()]
    if missing:
        raise SystemExit(f"Missing required outputs: {missing}")

    parsed = {f: _load_json_strict(OUT_DIR / f) for f in REQUIRED}

    # Minimal key checks
    for f in REQUIRED:
        obj = parsed[f]
        if not isinstance(obj, dict):
            raise SystemExit(f"{f} is not a JSON object")
        for key in ["pipeline_version", "generated_at_utc"]:
            if key not in obj:
                raise SystemExit(f"{f} missing required key: {key}")

    # Schema checks
    schema = parsed["schema.json"]
    if "udise_base_path" not in schema:
        raise SystemExit("schema.json missing udise_base_path")

    run_cfg = parsed.get("run_config.json")
    # run_config.json isn't required, but if present it should include input_mode.
    if (OUT_DIR / "run_config.json").exists():
        run_cfg = _load_json_strict(OUT_DIR / "run_config.json")
        input_mode = run_cfg.get("input_mode") if isinstance(run_cfg, dict) else None
        udise_data_path = (input_mode or {}).get("UDISE_DATA_PATH") if isinstance(input_mode, dict) else None
        udise_base_path = run_cfg.get("udise_base_path") if isinstance(run_cfg, dict) else None

        # Independence check: when UDISE_DATA_PATH is not provided, inputs must live under new_repo/data/
        if not udise_data_path and udise_base_path:
            ub = Path(str(udise_base_path)).resolve()
            try:
                ub.relative_to(DATA_DIR.resolve())
            except Exception:
                raise SystemExit(
                    "Independence check failed: UDISE_DATA_PATH not set, but udise_base_path is not under new_repo/data/. "
                    f"udise_base_path={ub} data_dir={DATA_DIR.resolve()}"
                )

    manifest = parsed["data_manifest.json"]
    files = manifest.get("files")
    if not isinstance(files, list) or not files:
        raise SystemExit("data_manifest.json must include non-empty files list")
    for entry in files[:5]:
        for k in ["path", "sha256", "bytes"]:
            if k not in entry:
                raise SystemExit(f"data_manifest.json file entry missing {k}")

    print("OK: contract check passed")


if __name__ == "__main__":
    main()
