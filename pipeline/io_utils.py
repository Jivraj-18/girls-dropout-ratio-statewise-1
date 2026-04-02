from __future__ import annotations

import hashlib
import json
import math
import csv
from pathlib import Path
from typing import Any


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sanitize_for_json(obj: Any) -> Any:
    if obj is None or isinstance(obj, (str, bool, int)):
        return obj

    if hasattr(obj, "item"):
        try:
            obj = obj.item()
        except Exception:
            pass

    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj

    if obj == "":
        return None

    if isinstance(obj, Path):
        return str(obj)

    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple)):
        return [sanitize_for_json(v) for v in obj]

    return obj


def write_json(path: Path, payload: Any) -> None:
    ensure_dir(path.parent)
    safe = sanitize_for_json(payload)
    path.write_text(
        json.dumps(safe, indent=2, ensure_ascii=False, allow_nan=False),
        encoding="utf-8",
    )


def write_csv(path: Path, _unused: Any) -> None:
    raise NotImplementedError("write_csv (pandas) is not used in this pipeline; use write_rows_csv")


def write_rows_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    """Write rows to CSV using stdlib only."""

    ensure_dir(path.parent)
    if fieldnames is None:
        keys: set[str] = set()
        for r in rows:
            keys.update(map(str, r.keys()))
        fieldnames = sorted(keys)

    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: sanitize_for_json(r.get(k)) for k in fieldnames})
