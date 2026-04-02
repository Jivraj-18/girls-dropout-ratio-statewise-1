from __future__ import annotations

from pathlib import Path

from pipeline.io_utils import sha256_file, write_json
from pipeline.json_utils import read_json
from pipeline.metadata import run_generated_at_utc
from pipeline.udise_loaders import (
    load_dropout_rates_all_years,
    load_female_teacher_share,
    load_infrastructure_table,
)
from pipeline.version import PIPELINE_VERSION, SCHEMA_VERSION


ROOT_DIR = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT_DIR / "out"


def _year_to_int(y: str) -> int:
    return int(str(y).split("-")[0])


def _latest_year(years: list[str]) -> str:
    return sorted(years, key=_year_to_int)[-1]


def _find_latest_year_with_features(udise_base: Path, years: list[str]) -> str | None:
    for y in sorted(years, key=_year_to_int, reverse=True):
        try:
            load_infrastructure_table(udise_base, y)
            load_female_teacher_share(udise_base, y)
            return y
        except Exception:
            continue
    return None


def main() -> None:
    run_cfg = read_json(OUT_DIR / "run_config.json")
    udise_base = Path(run_cfg["udise_base_path"]).expanduser().resolve()
    years = list(run_cfg.get("years_detected") or [])
    generated_at_utc = str(run_cfg.get("generated_at_utc") or run_generated_at_utc(OUT_DIR))

    dropout_long, dropout_schemas, dropout_files = load_dropout_rates_all_years(udise_base)

    feature_year = _find_latest_year_with_features(udise_base, years)
    infra_schema = {}
    female_schema = {}
    infra_files: list[Path] = []
    female_file: Path | None = None

    if feature_year:
        _, infra_schema, infra_files = load_infrastructure_table(udise_base, feature_year)
        _, female_schema, female_file = load_female_teacher_share(udise_base, feature_year)

    files_used: list[Path] = list(dropout_files.values())
    if infra_files:
        files_used.extend(infra_files)
    if female_file:
        files_used.append(female_file)

    manifest_entries = []
    for f in sorted(set(files_used), key=lambda p: str(p)):
        try:
            manifest_entries.append(
                {
                    "path": str(f),
                    "sha256": sha256_file(f),
                    "bytes": int(f.stat().st_size),
                }
            )
        except FileNotFoundError:
            continue

    schema_payload = {
        "pipeline_version": PIPELINE_VERSION,
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated_at_utc,
        "udise_base_path": str(udise_base),
        "years": sorted(years, key=_year_to_int),
        "tables": {
            "dropout": {"schemas": dropout_schemas},
            "infrastructure": {"year": feature_year, "schemas": infra_schema},
            "female_teachers": {"year": feature_year, "schemas": female_schema},
        },
        "row_counts": {
            "dropout_long_rows": int(len(dropout_long)),
        },
        "feature_year": feature_year,
    }

    write_json(OUT_DIR / "schema.json", schema_payload)
    write_json(
        OUT_DIR / "data_manifest.json",
        {
            "pipeline_version": PIPELINE_VERSION,
            "generated_at_utc": generated_at_utc,
            "udise_base_path": str(udise_base),
            "files": manifest_entries,
        },
    )


if __name__ == "__main__":
    main()
