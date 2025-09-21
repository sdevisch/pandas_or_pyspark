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
        else:
            # Fallback: synthesize a tiny parquet for this rows value
            try:
                import pandas as _pd  # type: ignore
                tmp_dir = ROOT / "data" / f"brc_tmp_{max(1, rows)}"
                tmp_dir.mkdir(parents=True, exist_ok=True)
                p = tmp_dir / "generated.parquet"
                if not p.exists():
                    r = __import__("random").Random(42)
                    n = max(1, rows)
                    pdf = _pd.DataFrame({
                        "id": list(range(n)),
                        "x": [r.randint(-10, 10) for _ in range(n)],
                        "y": [r.randint(-10, 10) for _ in range(n)],
                        "cat": [r.choice(["x", "y", "z"]) for _ in range(n)],
                    })
                    pdf.to_parquet(str(p), index=False)
                glob_arg = str(tmp_dir / "*.parquet")
            except Exception:
                glob_arg = None
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


