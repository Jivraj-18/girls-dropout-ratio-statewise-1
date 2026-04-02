from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RunConfig:
    udise_base: Path  # folder containing year directories with csv_files/


def resolve_udise_base() -> RunConfig:
    """Resolve input data location.

    Priority:
    1) UDISE_DATA_PATH env
    2) new_repo/data/udise_csv_data (gitignored)

    UDISE_DATA_PATH may point to either:
    - the udise_csv_data folder itself, OR
    - a parent folder that contains udise_csv_data.
    """

    here = Path(__file__).resolve()
    new_repo_root = here.parents[1]

    env = os.environ.get("UDISE_DATA_PATH")
    if env:
        p = Path(env).expanduser().resolve()
        if (p / "udise_csv_data").exists():
            return RunConfig(udise_base=(p / "udise_csv_data").resolve())
        return RunConfig(udise_base=p)

    # Default to new_repo-local downloaded data (gitignored).
    default = (new_repo_root / "data" / "udise_csv_data").resolve()
    return RunConfig(udise_base=default)
