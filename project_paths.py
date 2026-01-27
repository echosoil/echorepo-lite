# project_paths.py
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

# Files/folders that typically live at the repo root
_MARKERS = (".env", "echorepo", "tools", "scripts")


@lru_cache(maxsize=1)
def get_project_root(start: str | Path | None = None) -> Path:
    # 1) Explicit override wins (handy in Docker/CI)
    if env := os.getenv("PROJECT_ROOT"):
        return Path(env).resolve()

    # 2) Walk upward from this file until a marker is found
    here = Path(start or __file__).resolve()
    for p in (here, *here.parents):
        if any((p / m).exists() for m in _MARKERS):
            return p

    # 3) Fallback: current working dir
    return Path.cwd().resolve()


PROJECT_ROOT = get_project_root(__file__)
DOTENV_PATH = PROJECT_ROOT / ".env"


def load_env(override: bool = False) -> None:
    """Optional: load .env once for the whole app."""
    try:
        from dotenv import load_dotenv

        load_dotenv(DOTENV_PATH, override=override)
    except Exception:
        pass
