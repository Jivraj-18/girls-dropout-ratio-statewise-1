from __future__ import annotations

import re
import csv
from pathlib import Path
from typing import Optional

from .udise_utils import TableSchema, normalize_whitespace


_COLNUM_RE = re.compile(r"^\(\s*1\s*\)$")
def read_multiheader_csv(path: Path) -> tuple[list[list[str]], list[str], TableSchema]:
    """Read UDISE-extracted CSVs that often have multi-row headers.

    Returns:
      rows: list of data rows (list[str])
      columns: flattened unique column names
      schema: TableSchema snapshot
    """

    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.reader(f)
        raw_rows = [list(r) for r in reader]

    # Drop completely empty rows
    raw_rows = [r for r in raw_rows if any(normalize_whitespace(c) for c in r)]
    if not raw_rows:
        schema = TableSchema(
            file=str(path),
            header_rows=0,
            detected_columns=[],
            n_rows=0,
            n_cols=0,
            sample_states=[],
        )
        return [], [], schema

    max_cols = max(len(r) for r in raw_rows)
    padded = [r + [""] * (max_cols - len(r)) for r in raw_rows]

    header_end_idx: Optional[int] = None
    for idx in range(min(20, len(padded))):
        first_cell = padded[idx][0] if max_cols > 0 else ""
        if _COLNUM_RE.match(normalize_whitespace(first_cell)):
            header_end_idx = idx
            break
        if max_cols > 1:
            second_cell = padded[idx][1]
            if _COLNUM_RE.match(normalize_whitespace(second_cell)):
                header_end_idx = idx
                break

    if header_end_idx is None:
        header_end_idx = 0

    header_rows = list(range(header_end_idx))

    columns: list[str] = []
    for col_idx in range(max_cols):
        parts: list[str] = []
        for r in header_rows:
            cell = normalize_whitespace(padded[r][col_idx])
            if cell and cell.lower() not in {"nan", "none"}:
                parts.append(cell)
        col_name = normalize_whitespace(" ".join(parts)) if parts else f"col_{col_idx}"
        columns.append(col_name)

    seen: dict[str, int] = {}
    unique_columns: list[str] = []
    for c in columns:
        if c not in seen:
            seen[c] = 0
            unique_columns.append(c)
            continue
        seen[c] += 1
        unique_columns.append(f"{c}__{seen[c]}")

    data_rows = padded[header_end_idx + 1 :]
    # Normalize whitespace and drop fully empty rows
    out_rows: list[list[str]] = []
    for r in data_rows:
        nr = [normalize_whitespace(c) for c in r]
        if any(c for c in nr):
            out_rows.append(nr)

    sample_states: list[str] = []
    for r in out_rows[:10]:
        if r and normalize_whitespace(r[0]):
            sample_states.append(normalize_whitespace(r[0]))

    schema = TableSchema(
        file=str(path),
        header_rows=header_end_idx,
        detected_columns=unique_columns,
        n_rows=len(out_rows),
        n_cols=len(unique_columns),
        sample_states=sample_states,
    )
    return out_rows, unique_columns, schema
