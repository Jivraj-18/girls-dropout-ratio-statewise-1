from __future__ import annotations

import os
import shutil
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import urlopen

from pipeline.io_utils import write_json
from pipeline.run_config import resolve_udise_base
from pipeline.version import PIPELINE_VERSION


ROOT_DIR = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT_DIR / "out"

DEFAULT_GIT_REPO_URL = "https://github.com/gsidhu/udise-csv-data"

_YEAR_TOKEN_RE = re.compile(r"(20\d{2}-\d{2})")


def _extract_year_token(name: str) -> str | None:
    m = _YEAR_TOKEN_RE.search(str(name))
    return m.group(1) if m else None


def _list_years(udise_base: Path) -> list[str]:
    years: list[str] = []
    if not udise_base.exists():
        return years
    for child in udise_base.iterdir():
        if child.is_dir() and (child / "csv_files").exists():
            tok = _extract_year_token(child.name)
            if tok:
                years.append(tok)
    return sorted(set(years))


def _download_to(path: Path, url: str, timeout_sec: int = 120) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with urlopen(url, timeout=timeout_sec) as r:  # nosec - controlled URL via env/default
        tmp = path.with_suffix(path.suffix + ".part")
        with tmp.open("wb") as f:
            shutil.copyfileobj(r, f)
        tmp.replace(path)


def _github_zip_urls(repo_url: str) -> list[str]:
    """Return candidate GitHub zip URLs for common default branches."""

    # Accept https://github.com/<owner>/<repo> (optionally with trailing slash or .git)
    u = repo_url.strip()
    if u.endswith(".git"):
        u = u[: -len(".git")]
    u = u.rstrip("/")

    parsed = urlparse(u)
    if parsed.netloc != "github.com":
        raise ValueError("Only github.com repo URLs are supported")

    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) < 2:
        raise ValueError("Invalid GitHub repo URL")
    owner, repo = parts[0], parts[1]

    base = f"https://github.com/{owner}/{repo}"
    return [
        f"{base}/archive/refs/heads/main.zip",
        f"{base}/archive/refs/heads/master.zip",
    ]


def _extract_zip(zip_path: Path, extract_dir: Path) -> None:
    import zipfile

    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(extract_dir)


def _find_udise_base_dir(root: Path) -> Path:
    """Find the directory that contains year folders with csv_files.

    Supports layouts like:
      <extract>/<repo>-<branch>/UDISE 2018-19/csv_files/
    """

    # If root itself looks like the base, use it.
    if _list_years(root):
        return root

    # Otherwise find the first directory (shallow) that has multiple year folders.
    candidates: list[Path] = []
    for d in root.glob("**"):
        if not d.is_dir():
            continue
        ys = _list_years(d)
        if len(ys) >= 2:
            candidates.append(d)

    if not candidates:
        raise FileNotFoundError(f"Could not find a UDISE base dir under extracted folder: {root}")

    candidates.sort(key=lambda p: (len(str(p)), str(p)))
    return candidates[0]


def main() -> None:
    cfg = resolve_udise_base()

    # Source selection rules:
    # - If UDISE_DATA_PATH is set, use it (no downloads).
    # - Else: download+extract from GitHub (repo URL defaulted to DEFAULT_GIT_REPO_URL).
    env_data_path = os.environ.get("UDISE_DATA_PATH")
    zip_url = os.environ.get("UDISE_CSV_ZIP_URL")
    git_repo_url = os.environ.get("UDISE_GIT_REPO_URL", DEFAULT_GIT_REPO_URL)

    data_dir = ROOT_DIR / "data"
    extract_root = data_dir / "extract"

    if not env_data_path:
        # Download zip (explicit URL takes precedence, else build from repo URL)
        if zip_url:
            candidate_urls = [zip_url]
        else:
            candidate_urls = _github_zip_urls(git_repo_url)

        zip_path = data_dir / "udise_source.zip"
        data_dir.mkdir(parents=True, exist_ok=True)

        last_err: Exception | None = None
        for u in candidate_urls:
            try:
                _download_to(zip_path, u, timeout_sec=180)
                last_err = None
                break
            except HTTPError as e:
                last_err = e
                continue
            except Exception as e:
                last_err = e
                continue

        if last_err is not None:
            raise SystemExit(f"Failed to download UDISE zip from {candidate_urls}: {last_err}")

        # Clean extract root for determinism
        if extract_root.exists():
            shutil.rmtree(extract_root)
        _extract_zip(zip_path, extract_root)

        udise_base = _find_udise_base_dir(extract_root)
        cfg = type(cfg)(udise_base=udise_base.resolve())

    years = _list_years(cfg.udise_base)

    payload = {
        "pipeline_version": PIPELINE_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "udise_base_path": str(cfg.udise_base),
        "years_detected": years,
        "input_mode": {
            "UDISE_DATA_PATH": os.environ.get("UDISE_DATA_PATH"),
            "UDISE_CSV_ZIP_URL": os.environ.get("UDISE_CSV_ZIP_URL"),
            "UDISE_GIT_REPO_URL": os.environ.get("UDISE_GIT_REPO_URL", DEFAULT_GIT_REPO_URL),
        },
    }

    write_json(OUT_DIR / "run_config.json", payload)

    if not years:
        raise SystemExit(
            "No UDISE year folders found. Set UDISE_DATA_PATH to a folder that contains year/csv_files." \
            f" Looked under: {cfg.udise_base}"
        )


if __name__ == "__main__":
    main()
