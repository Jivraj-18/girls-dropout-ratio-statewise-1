from __future__ import annotations

from pathlib import Path

from pipeline.io_utils import write_json
from pipeline.json_utils import read_json
from pipeline.metadata import run_generated_at_utc
from pipeline.udise_loaders import load_dropout_rates_all_years
from pipeline.version import PIPELINE_VERSION


ROOT_DIR = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT_DIR / "out"


def _year_to_int(y: str) -> int:
    return int(str(y).split("-")[0])


def main() -> None:
    run_cfg = read_json(OUT_DIR / "run_config.json")
    udise_base = Path(run_cfg["udise_base_path"]).expanduser().resolve()
    generated_at_utc = str(run_cfg.get("generated_at_utc") or run_generated_at_utc(OUT_DIR))

    dropout_long, _, _ = load_dropout_rates_all_years(udise_base)

    # Keep only girls (core focus of this repo/pipeline)
    rows = [r for r in dropout_long if str(r.get("gender")) == "Girls"]

    def sk(r: dict) -> tuple:
        y = str(r.get("year") or "")
        y0 = _year_to_int(y) if y else 0
        return (y0, str(r.get("state_ut") or ""), str(r.get("level") or ""))

    rows.sort(key=sk)

    write_json(OUT_DIR / "dropout_girls_long.json", {"rows": rows})

    write_json(
        OUT_DIR / "preprocess_audit.json",
        {
            "pipeline_version": PIPELINE_VERSION,
            "generated_at_utc": generated_at_utc,
            "udise_base_path": str(udise_base),
            "rows_dropout_girls": int(len(rows)),
            "years": sorted({str(r.get("year")) for r in rows if r.get("year")}, key=_year_to_int),
        },
    )


if __name__ == "__main__":
    main()
