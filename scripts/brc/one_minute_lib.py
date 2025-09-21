from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Optional, Tuple

ROOT = Path(__file__).resolve().parents[2]
PY = sys.executable or "python3"
SCRIPT = ROOT / "scripts" / "brc" / "billion_row_challenge.py"


def can_process_within(backend: str, rows: int, budget_s: float, operation: str | None = None, data_glob: str | None = None) -> Tuple[bool, float]:
    env = os.environ.copy()
    env["UNIPANDAS_BACKEND"] = backend
    glob_arg = data_glob
    if not glob_arg:
        # Prefer pre-generated parquet globs when available
        scales_dir = ROOT / "data" / "brc_scales" / f"parquet_{rows}"
        if scales_dir.exists() and any(scales_dir.glob("*.parquet")):
            glob_arg = str(scales_dir / "*.parquet")
    cmd = [PY, str(SCRIPT)]
    if glob_arg:
        cmd += ["--data-glob", glob_arg]
    cmd += ["--only-backend", backend, "--materialize", "count", "--no-md"]
    start = __import__("time").perf_counter()
    try:
        subprocess.run(cmd, env=env, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, timeout=budget_s)
        elapsed = (__import__("time").perf_counter() - start)
        return True, elapsed
    except Exception:
        elapsed = (__import__("time").perf_counter() - start)
        return False, elapsed


