from __future__ import annotations

import math
from collections import defaultdict
from pathlib import Path

from pipeline.io_utils import write_json
from pipeline.json_utils import read_json
from pipeline.metadata import run_generated_at_utc
from pipeline.stats_utils import corr_with_perm_test
from pipeline.version import PIPELINE_VERSION


ROOT_DIR = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT_DIR / "out"


def _year_to_int(y: str) -> int:
    return int(str(y).split("-")[0])


def _latest_year(years: list[str]) -> str:
    return sorted(set(years), key=_year_to_int)[-1]


def _quantile(sorted_vals: list[float], q: float) -> float | None:
    if not sorted_vals:
        return None
    if q <= 0:
        return float(sorted_vals[0])
    if q >= 1:
        return float(sorted_vals[-1])
    n = len(sorted_vals)
    pos = (n - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(sorted_vals[lo])
    w = pos - lo
    return float(sorted_vals[lo] * (1 - w) + sorted_vals[hi] * w)


def _mean(vals: list[float]) -> float | None:
    if not vals:
        return None
    return float(sum(vals) / len(vals))


def _median(sorted_vals: list[float]) -> float | None:
    return _quantile(sorted_vals, 0.5)


def _linear_slope(x: list[float], y: list[float]) -> float | None:
    if len(x) != len(y) or len(x) < 2:
        return None
    mx = sum(x) / len(x)
    my = sum(y) / len(y)
    x0 = [v - mx for v in x]
    denom = sum(v * v for v in x0)
    if denom == 0.0:
        return None
    num = sum(a * (b - my) for a, b in zip(x0, y))
    return float(num / denom)


def main() -> None:
    generated_at_utc = run_generated_at_utc(OUT_DIR)

    dropout_blob = read_json(OUT_DIR / "dropout_girls_long.json")
    rows: list[dict] = list(dropout_blob.get("rows") or [])

    years = sorted({str(r.get("year")) for r in rows if r.get("year")}, key=_year_to_int)
    if not years:
        raise SystemExit("No dropout rows found")

    focus_year = _latest_year(years)

    # Summary by (year, level)
    bucket: dict[tuple[str, str], list[float]] = defaultdict(list)
    for r in rows:
        year = r.get("year")
        level = r.get("level")
        rate = r.get("rate")
        if year is None or level is None:
            continue
        if isinstance(rate, (int, float)) and math.isfinite(float(rate)):
            bucket[(str(year), str(level))].append(float(rate))

    summary = []
    for (year, level), vals in sorted(bucket.items(), key=lambda kv: (_year_to_int(kv[0][0]), kv[0][1])):
        svals = sorted(vals)
        summary.append(
            {
                "year": year,
                "level": level,
                "n": len(vals),
                "mean": _mean(vals),
                "median": _median(svals),
                "min": float(svals[0]) if svals else None,
                "max": float(svals[-1]) if svals else None,
                "p10": _quantile(svals, 0.10),
                "p90": _quantile(svals, 0.90),
            }
        )

    # State trend slopes for secondary
    secondary = [r for r in rows if str(r.get("level")) == "Secondary (9-10)" and r.get("rate") is not None]
    sec_by_state: dict[str, list[tuple[int, float]]] = defaultdict(list)
    for r in secondary:
        state = str(r.get("state_ut") or "")
        year = str(r.get("year") or "")
        rate = r.get("rate")
        if not state or not year:
            continue
        if not isinstance(rate, (int, float)) or not math.isfinite(float(rate)):
            continue
        sec_by_state[state].append((_year_to_int(year), float(rate)))

    slopes = []
    for state, pts in sec_by_state.items():
        pts = sorted(pts, key=lambda t: t[0])
        x = [float(p[0]) for p in pts]
        y = [float(p[1]) for p in pts]
        slope = _linear_slope(x, y)
        if slope is None:
            continue
        slopes.append({"state_ut": state, "slope_pp_per_year": slope, "n_years": len(set(x))})

    slopes.sort(key=lambda r: (r["slope_pp_per_year"], r["state_ut"]))
    improving_fastest = slopes[:10]
    worsening_fastest = list(reversed(slopes[-10:]))

    # Feature importance on latest feature year (if present)
    features_blob = read_json(OUT_DIR / "features_latest.json") if (OUT_DIR / "features_latest.json").exists() else {"rows": []}
    features_rows: list[dict] = list(features_blob.get("rows") or [])

    feature_rows = []
    if features_rows:
        feature_year = str(features_rows[0].get("year") or focus_year)

        sec_latest = {
            str(r.get("state_ut")): r.get("rate")
            for r in secondary
            if str(r.get("year")) == feature_year and r.get("state_ut")
        }

        merged = []
        for fr in features_rows:
            state = fr.get("state_ut")
            if not state or state == "India":
                continue
            target = sec_latest.get(str(state))
            if not isinstance(target, (int, float)) or not math.isfinite(float(target)):
                continue
            merged.append({**fr, "secondary_dropout": float(target)})

        for feat in ["functional_girls_toilet_pct", "female_teacher_share"]:
            x = []
            y = []
            for m in merged:
                xv = m.get(feat)
                yv = m.get("secondary_dropout")
                if isinstance(xv, (int, float)) and math.isfinite(float(xv)):
                    x.append(float(xv))
                    y.append(float(yv))
            if len(x) < 5:
                continue
            corr = corr_with_perm_test(x, y, n_perm=5000, seed=42)
            imp = abs(corr.corr) if math.isfinite(corr.corr) else None
            feature_rows.append(
                {
                    "feature": feat,
                    "n": corr.n,
                    "corr": corr.corr,
                    "p_perm": corr.p_perm,
                    "importance_abs_corr": imp,
                }
            )

        feature_rows.sort(
            key=lambda r: (
                r.get("importance_abs_corr") is None,
                -(r.get("importance_abs_corr") or 0.0),
                str(r.get("feature") or ""),
            )
        )

    write_json(
        OUT_DIR / "summary_stats.json",
        {
            "pipeline_version": PIPELINE_VERSION,
            "generated_at_utc": generated_at_utc,
            "years": years,
            "focus_year": focus_year,
            "summary_by_year_level": summary,
        },
    )

    write_json(
        OUT_DIR / "analysis_results.json",
        {
            "pipeline_version": PIPELINE_VERSION,
            "generated_at_utc": generated_at_utc,
            "focus_year": focus_year,
            "secondary_trend_slopes": {
                "improving_fastest": improving_fastest,
                "worsening_fastest": worsening_fastest,
                "n_states": len(slopes),
            },
        },
    )

    write_json(
        OUT_DIR / "feature_importance.json",
        {
            "pipeline_version": PIPELINE_VERSION,
            "generated_at_utc": generated_at_utc,
            "method": "abs(Pearson corr) with permutation p-value (seed=42)",
            "target": "Secondary (9-10) girls dropout rate",
            "features_ranked": feature_rows,
        },
    )


if __name__ == "__main__":
    main()
