from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from pipeline.json_utils import read_json


def run_generated_at_utc(out_dir: Path) -> str:
    """Return a stable run timestamp.

    Preference order:
    1) out/run_config.json (written by acquire_data.py)
    2) out/schema.json (written by schema_discovery.py)
    3) current time (fallback)

    This keeps outputs byte-identical when re-running downstream stages.
    """

    for fname in ("run_config.json", "schema.json"):
        p = out_dir / fname
        if not p.exists():
            continue
        try:
            obj = read_json(p)
        except Exception:
            continue
        if isinstance(obj, dict):
            ts = obj.get("generated_at_utc")
            if isinstance(ts, str) and ts:
                return ts

    return datetime.now(timezone.utc).isoformat()
