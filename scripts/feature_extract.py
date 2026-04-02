from __future__ import annotations

from pathlib import Path

from pipeline.io_utils import write_json
from pipeline.json_utils import read_json
from pipeline.metadata import run_generated_at_utc
from pipeline.udise_loaders import load_female_teacher_share, load_infrastructure_table
from pipeline.version import PIPELINE_VERSION


ROOT_DIR = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT_DIR / "out"


def main() -> None:
    schema = read_json(OUT_DIR / "schema.json")
    udise_base = Path(schema["udise_base_path"]).expanduser().resolve()
    feature_year = schema.get("feature_year")
    generated_at_utc = str(schema.get("generated_at_utc") or run_generated_at_utc(OUT_DIR))

    if not feature_year:
        # Produce empty-but-valid artifacts.
        write_json(OUT_DIR / "features_latest.json", {"rows": []})
        write_json(
            OUT_DIR / "feature_extract_audit.json",
            {
                "pipeline_version": PIPELINE_VERSION,
                "generated_at_utc": generated_at_utc,
                "feature_year": None,
                "notes": "No year had both infrastructure (Table 2.5) and female teachers (Table 4.13) available.",
            },
        )
        return

    infra_rows, _, _ = load_infrastructure_table(udise_base, feature_year)
    ft_rows, _, _ = load_female_teacher_share(udise_base, feature_year)

    # Find functional girls' toilet column (count of schools)
    all_keys: set[str] = set()
    for r in infra_rows:
        all_keys.update(r.keys())
    cols = {c: str(c).lower() for c in all_keys}
    fg_candidates = [c for c, cl in cols.items() if "functional" in cl and "girls" in cl and "toilet" in cl]
    total_candidates = [c for c, cl in cols.items() if cl == "total_schools" or ("total" in cl and "school" in cl)]

    if not fg_candidates:
        raise ValueError("No 'Functional Girls' Toilet' column found in infrastructure table")

    fg_col = fg_candidates[0]
    total_col = total_candidates[0] if total_candidates else "total_schools"

    ft_by_state = {r.get("state_ut"): r for r in ft_rows if r.get("state_ut")}

    features: list[dict] = []
    for r in infra_rows:
        state = r.get("state_ut")
        if not state:
            continue
        functional = r.get(fg_col)
        total = r.get(total_col)
        pct = None
        if functional is not None and total not in {None, 0.0}:
            pct = 100.0 * float(functional) / float(total)

        rec = {
            "year": feature_year,
            "state_ut": state,
            "functional_girls_toilets": functional,
            "total_schools": total,
            "functional_girls_toilet_pct": pct,
        }
        ft = ft_by_state.get(state)
        if ft:
            rec["female_teacher_share"] = ft.get("female_teacher_share")
        else:
            rec["female_teacher_share"] = None
        features.append(rec)

    # Include states present only in ft
    infra_states = {r.get("state_ut") for r in infra_rows if r.get("state_ut")}
    for state, ft in ft_by_state.items():
        if state in infra_states:
            continue
        features.append(
            {
                "year": feature_year,
                "state_ut": state,
                "functional_girls_toilets": None,
                "total_schools": None,
                "functional_girls_toilet_pct": None,
                "female_teacher_share": ft.get("female_teacher_share"),
            }
        )

    features.sort(key=lambda rr: str(rr.get("state_ut") or ""))
    write_json(OUT_DIR / "features_latest.json", {"rows": features})

    write_json(
        OUT_DIR / "feature_extract_audit.json",
        {
            "pipeline_version": PIPELINE_VERSION,
            "generated_at_utc": generated_at_utc,
            "feature_year": feature_year,
            "rows": int(len(features)),
            "features": ["functional_girls_toilet_pct", "female_teacher_share"],
        },
    )


if __name__ == "__main__":
    main()
