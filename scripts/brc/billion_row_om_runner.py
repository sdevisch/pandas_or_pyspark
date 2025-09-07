#!/usr/bin/env python3
"""
Order-of-magnitude Billion Row Challenge (BRC) runner.

Purpose
-------
Run a small, repeatable subset of the Billion Row Challenge at increasing data
sizes (1e3, 1e4, 1e5, 1e6 rows by default) across multiple backends (pandas,
Dask, pandas-on-Spark). For each (backend, size), we:
1) Delegate to the existing `billion_row_challenge.py` to generate/load data
   and perform a simple operation (default: filter)
2) Impose a per-step wall-clock budget (default: 180s) using subprocess timeout
3) Parse the generated report to capture timing metrics
4) Stop escalating for a backend once a step exceeds the budget (fails)

Output
------
Writes a fixed-width table to `reports/billion_row_om.md` with columns:
backend, rows, read_s, compute_s, ok

Notes
-----
- We intentionally re-use the underlying script rather than duplicate logic,
  to keep a single source of truth for data generation and per-backend ops.
- We pass the backend selection via environment variable `UNIPANDAS_BACKEND`.
  This relies on the library's backend detection honoring that env var.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[2]
REPORTS = ROOT / "reports" / "brc"
# Ensure the reports directory exists so downstream writes do not fail
REPORTS.mkdir(parents=True, exist_ok=True)
OUT = REPORTS / "billion_row_om.md"

PY = sys.executable or "python3"  # Use current interpreter; fallback to python3
SCRIPT = ROOT / "scripts" / "brc" / "billion_row_challenge.py"  # Delegate script path

Backends = ["pandas", "dask", "pyspark", "polars", "duckdb"]  # Execution backends under test
ORDERS = [1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000]  # Escalating sizes (rows)
DEFAULT_BUDGET_S = 180.0  # Per (backend,size) wall-clock budget in seconds


@dataclass
class Entry:
    """Single (backend, size) measurement.

    Attributes
    ----------
    backend: Name of the backend (e.g., "pandas", "dask", "pyspark").
    rows: Integer number of rows attempted for this run.
    read_s: Seconds spent in the read/concat stage (as parsed from report).
    compute_s: Seconds spent computing the operation (as parsed from report).
    ok: True if step completed within time budget; False if timed out/failed.
    """

    backend: str
    rows: int
    read_s: Optional[float]
    compute_s: Optional[float]
    ok: bool
    input_rows: Optional[int] = None


def _count_input_rows(rows: int, data_glob: Optional[str]) -> Optional[int]:
    if data_glob:
        import glob as _glob
        import os as _os
        import pyarrow.parquet as _pq  # type: ignore
        count = 0
        for p in _glob.glob(data_glob):
            try:
                count += int(_pq.ParquetFile(p).metadata.num_rows)
            except Exception:
                # Fallback: approximate by counting lines minus header for CSV
                if p.lower().endswith('.csv'):
                    with open(p, 'r') as f:
                        count += max(0, sum(1 for _ in f) - 1)
        return count
    return rows


def _parquet_glob(size: int) -> str:
    return str(ROOT / f"data/brc_{size}" / "*.parquet")


def run_once(backend: str, rows: int, budget_s: float) -> Entry:
    """Run one (backend, size) step and parse read/compute timings."""
    env = _env_for_backend(backend)
    cmd = _cmd_for_rows(backend, rows)
    ok, read_s, compute_s = _run_challenge(cmd, env, budget_s, backend)
    if not ok:
        return Entry(backend=backend, rows=rows, read_s=None, compute_s=None, ok=False)
    return Entry(backend=backend, rows=rows, read_s=read_s, compute_s=compute_s, ok=True, input_rows=_count_input_rows(rows, None))


def _env_for_backend(backend: str) -> dict:
    env = os.environ.copy()
    env["UNIPANDAS_BACKEND"] = backend
    return env


def _cmd_for_rows(backend: str, rows: int) -> List[str]:
    return [PY, str(SCRIPT), "--rows-per-chunk", str(rows), "--num-chunks", "1", "--operation", "filter", "--only-backend", backend]


def _cmd_for_glob(backend: str, glob_path: str) -> List[str]:
    return [PY, str(SCRIPT), "--data-glob", glob_path, "--operation", "filter", "--only-backend", backend]


def _parse_latest_timings(backend: str) -> tuple[Optional[float], Optional[float]]:
    p = REPORTS / "billion_row_challenge.md"
    if not p.exists():
        return None, None
    for line in p.read_text().strip().splitlines()[::-1]:
        if line.startswith(backend):
            parts = line.split()
            if len(parts) >= 5:
                try:
                    return float(parts[3]), float(parts[4])
                except Exception:
                    return None, None
    return None, None


def _run_challenge(cmd: List[str], env: dict, budget_s: float, backend: str) -> tuple[bool, Optional[float], Optional[float]]:
    try:
        subprocess.run(cmd, env=env, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=budget_s)
        r_s, c_s = _parse_latest_timings(backend)
        return True, r_s, c_s
    except subprocess.TimeoutExpired:
        return False, None, None
    except Exception:
        return False, None, None

def _build_row(entry: Entry, size: int) -> List[str]:
    rs = f"{entry.read_s:.4f}" if entry.read_s is not None else "-"
    cs = f"{entry.compute_s:.4f}" if entry.compute_s is not None else "-"
    ir = f"{entry.input_rows}" if entry.input_rows is not None else "-"
    ok = "yes" if entry.ok else "no"
    return [entry.backend, f"{size:.1e}", rs, cs, ir, ok]


def _default_headers() -> List[str]:
    return ["backend", "rows(sci)", "read_s", "compute_s", "input_rows", "ok"]


def _entry_for_backend_size(backend: str, size: int, budget: float) -> Entry:
    pg = _parquet_glob(size)
    if __import__("glob").glob(pg):
        env = _env_for_backend(backend)
        ok, rs, cs = _run_challenge(_cmd_for_glob(backend, pg), env, budget, backend)
        return Entry(backend=backend, rows=size, read_s=rs, compute_s=cs, ok=bool(ok), input_rows=_count_input_rows(size, pg) if ok else None)
    return run_once(backend, size, budget)


def _collect_rows(budget: float) -> List[List[str]]:
    rows: List[List[str]] = []
    for backend in Backends:
        for size in ORDERS:
            entry = _entry_for_backend_size(backend, size, budget)
            rows.append(_build_row(entry, size))
            if not entry.ok:
                break
    return rows


def _write_rows(rows: List[List[str]]) -> None:
    try:
        import sys as _sys
        from pathlib import Path as _Path
        _here = _Path(__file__).resolve()
        _sys.path.append(str(_here.parents[1]))  # scripts
        from utils import write_fixed_markdown as _write_md  # type: ignore
        _write_md(OUT, "Billion Row OM Runner", _default_headers(), rows, preface_lines=None, right_align_from=2)
    except Exception:
        # Fallback minimal TSV-like output
        lines = ["# Billion Row OM Runner", "", "```text", "\t".join(_default_headers())]
        for r in rows:
            lines.append("\t".join(str(x) for x in r))
        lines.extend(["```", ""])
        OUT.write_text("\n".join(lines))
        print("Wrote", OUT)


def main():
    parser = argparse.ArgumentParser(description="Order-of-magnitude BRC runner with per-step cap")
    parser.add_argument("--budgets", type=float, default=DEFAULT_BUDGET_S)
    args = parser.parse_args()
    rows = _collect_rows(float(args.budgets))
    _write_rows(rows)


if __name__ == "__main__":
    sys.exit(main())
