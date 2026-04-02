from __future__ import annotations

import re
from dataclasses import asdict
from pathlib import Path

from .udise_readers import read_multiheader_csv
from .udise_utils import find_first_matching_file, normalize_state_ut_name, normalize_whitespace, to_number


_YEAR_TOKEN_RE = re.compile(r"(20\d{2}-\d{2})")


def _extract_year_token(name: str) -> str | None:
    m = _YEAR_TOKEN_RE.search(str(name))
    return m.group(1) if m else None


def year_dirs(udise_base: Path) -> list[tuple[str, Path]]:
    """Return (canonical_year, path) pairs for directories containing csv_files."""

    years: list[tuple[str, Path]] = []
    for child in udise_base.iterdir():
        if not child.is_dir() or not (child / "csv_files").exists():
            continue
        tok = _extract_year_token(child.name)
        if not tok:
            continue
        years.append((tok, child))

    years.sort(key=lambda yp: int(yp[0].split("-")[0]))
    return years


def find_year_dir(udise_base: Path, year: str) -> Path:
    for tok, p in year_dirs(udise_base):
        if tok == year:
            return p
    raise FileNotFoundError(f"No year directory found for {year} under {udise_base}")


def load_dropout_rates_all_years(
    udise_base: Path,
) -> tuple[list[dict], dict[str, dict], dict[str, Path]]:
    """Return long rows: year, state_ut, level, gender, rate."""

    schemas: dict[str, dict] = {}
    files_used: dict[str, Path] = {}
    out: list[dict] = []

    for year, year_dir in year_dirs(udise_base):
        csv_dir = year_dir / "csv_files"

        dropout_file = find_first_matching_file(
            csv_dir,
            patterns=[
                "*Table 5.13*Dropout Rate*gender*.csv",
                "*Table 6.13*Dropout Rate*gender*.csv",
                "*Dropout Rate by level*gender*.csv",
            ],
        )
        files_used[year] = dropout_file

        rows, cols, schema = read_multiheader_csv(dropout_file)
        schemas[f"dropout::{year}"] = asdict(schema)

        # Expect: 0 State + 9 values (Primary/Upper Primary/Secondary x Boys/Girls/Total)
        for r in rows:
            if len(r) < 10:
                continue
            state = normalize_state_ut_name(r[0])
            if not state or state.lower() == "nan":
                continue

            # Keep first 10 columns; ignore stray extras
            values = r[:10]
            metrics = {
                "Primary (1-5)": {
                    "Boys": to_number(values[1]),
                    "Girls": to_number(values[2]),
                    "Total": to_number(values[3]),
                },
                "Upper Primary (6-8)": {
                    "Boys": to_number(values[4]),
                    "Girls": to_number(values[5]),
                    "Total": to_number(values[6]),
                },
                "Secondary (9-10)": {
                    "Boys": to_number(values[7]),
                    "Girls": to_number(values[8]),
                    "Total": to_number(values[9]),
                },
            }

            for level, genders in metrics.items():
                for gender, rate in genders.items():
                    out.append(
                        {
                            "year": year,
                            "state_ut": state,
                            "level": level,
                            "gender": gender,
                            "rate": rate,
                        }
                    )

    def sort_key(row: dict) -> tuple:
        y = str(row.get("year") or "")
        y0 = int(y.split("-")[0]) if y and y.split("-")[0].isdigit() else 0
        return (y0, str(row.get("state_ut") or ""), str(row.get("level") or ""), str(row.get("gender") or ""))

    out.sort(key=sort_key)
    return out, schemas, files_used


def load_infrastructure_table(
    udise_base: Path,
    year: str,
) -> tuple[list[dict], dict[str, dict], list[Path]]:
    """Load Table 2.5 infrastructure for a given year. Returns wide rows keyed by state_ut."""

    year_dir = find_year_dir(udise_base, year) / "csv_files"
    files = sorted(year_dir.glob("*Table 2.5*Infrastructure*page*.csv"))
    if not files:
        files = sorted(year_dir.glob("*Table 2.5*Infrastructure*.csv"))
    if not files:
        raise FileNotFoundError(f"No Table 2.5 Infrastructure files found for {year} in {year_dir}")

    schemas: dict[str, dict] = {}

    by_state: dict[str, dict] = {}

    for f in files:
        rows, cols, schema = read_multiheader_csv(f)
        schemas[f"infra::{year}::{Path(f).name}"] = asdict(schema)

        # Identify state column by header fragments
        norm = [normalize_whitespace(str(c)).lower() for c in cols]
        state_idx = 0
        for i, cl in enumerate(norm):
            if ("state" in cl and "ut" in cl) or cl in {"state/ut", "state /ut", "india/state/ut", "india/state /ut"}:
                state_idx = i
                break

        for r in rows:
            if len(r) <= state_idx:
                continue
            state = normalize_state_ut_name(r[state_idx])
            if not state or state.lower() == "nan":
                continue

            rec = by_state.get(state, {"state_ut": state})

            for i, col_name in enumerate(cols):
                if i == state_idx:
                    continue
                # Do not overwrite existing non-empty value
                if col_name in rec and rec[col_name] not in {None, ""}:
                    continue
                rec[col_name] = to_number(r[i])

            by_state[state] = rec

    # Standardize total schools column name if possible
    for state, rec in list(by_state.items()):
        if "total_schools" in rec:
            continue
        candidates = [k for k in rec.keys() if ("total" in str(k).lower() and "school" in str(k).lower())]
        if candidates:
            rec["total_schools"] = rec.get(candidates[0])
            by_state[state] = rec

    rows_out = sorted(by_state.values(), key=lambda r: str(r.get("state_ut") or ""))
    return rows_out, schemas, files


def load_female_teacher_share(
    udise_base: Path,
    year: str,
) -> tuple[list[dict], dict[str, dict], Path]:
    """Load Table 4.13 (All management) and compute female teacher share by state."""

    csv_dir = find_year_dir(udise_base, year) / "csv_files"
    file_path = find_first_matching_file(
        csv_dir,
        patterns=["*Table 4.13*Number of teachers by management, gender and classes taught*All Management*.csv"],
    )

    rows, cols, schema = read_multiheader_csv(file_path)

    out: list[dict] = []
    for r in rows:
        if len(r) < 4:
            continue
        state = normalize_state_ut_name(r[0])
        if not state or state.lower() == "nan":
            continue

        male = to_number(r[1])
        female = to_number(r[2])
        total = to_number(r[3])
        share = (female / total) if (female is not None and total not in {None, 0.0}) else None

        out.append(
            {
                "year": year,
                "state_ut": state,
                "teachers_male": male,
                "teachers_female": female,
                "teachers_total": total,
                "female_teacher_share": share,
            }
        )

    out.sort(key=lambda r: str(r.get("state_ut") or ""))
    return out, {f"female_teachers::{year}": asdict(schema)}, file_path
