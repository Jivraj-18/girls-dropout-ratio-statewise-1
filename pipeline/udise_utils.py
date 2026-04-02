from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


_WHITESPACE_RE = re.compile(r"\s+")


def normalize_whitespace(text: str) -> str:
    return _WHITESPACE_RE.sub(" ", text.replace("\u00a0", " ").strip())


_STATE_ALIASES = {
    "Daman and Diu and Dadra and Nagar Haveli": "Dadra and Nagar Haveli and Daman and Diu",
    "Dadra and Nagar Haveli": "Dadra and Nagar Haveli and Daman and Diu",
    "Daman and Diu": "Dadra and Nagar Haveli and Daman and Diu",
}


def normalize_state_ut_name(raw: str) -> str:
    s = normalize_whitespace(str(raw))
    s = s.strip('"')
    s = s.replace("/", " ")
    s = normalize_whitespace(s)
    return _STATE_ALIASES.get(s, s)


def find_first_matching_file(base_dir: Path, patterns: Iterable[str]) -> Path:
    matches: list[Path] = []
    for pattern in patterns:
        matches.extend(sorted(base_dir.glob(pattern)))
    if not matches:
        raise FileNotFoundError(f"No files matched patterns under {base_dir}: {list(patterns)}")
    return matches[0]


def to_number(value: str | None) -> float | None:
    if value is None:
        return None
    s = normalize_whitespace(str(value))
    if s in {"", "-", "–", "—"}:
        return None
    sl = s.lower()
    if sl in {"nan", "none"}:
        return None
    s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


@dataclass(frozen=True)
class TableSchema:
    file: str
    header_rows: int
    detected_columns: list[str]
    n_rows: int
    n_cols: int
    sample_states: list[str]
