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
import concurrent.futures  # Reserved for potential parallelization (not used yet)
import os
import signal  # Not strictly required now; kept for future fine-grained control
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
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
    """Execute a single step for a given backend and size with a time budget.

    We spawn the underlying BRC scaffold as a subprocess so we can set a
    wall-clock timeout. The subprocess writes a timing report which we parse.
    """

    # Prepare environment: select backend via env var consumed by library
    env = os.environ.copy()
    env["UNIPANDAS_BACKEND"] = backend

    # Build command line to run one-chunk filter scenario at the given size
    cmd = [
        PY,
        str(SCRIPT),
        "--rows-per-chunk",
        str(rows),
        "--num-chunks",
        "1",
        "--operation",
        "filter",
        "--only-backend",
        backend,
    ]
    start = time.time()
    try:
        # Run the subprocess with a hard timeout; capture output for diagnostics
        subprocess.run(
            cmd,
            env=env,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=budget_s,
        )
        # Parse the latest report file to extract last line timings for this backend
        report_path = REPORTS / "billion_row_challenge.md"
        read_s = None
        compute_s = None
        if report_path.exists():
            lines = report_path.read_text().strip().splitlines()
            # find header values to compute MB/s
            total_bytes = None
            for i, line in enumerate(lines[::-1]):
                if line.startswith("- total_bytes:"):
                    try:
                        total_bytes = int(line.split(":", 1)[1].strip())
                    except Exception:
                        total_bytes = None
                if line.startswith(backend):
                    parts = line.split()
                    if len(parts) >= 6:
                        try:
                            read_s = float(parts[3])
                            compute_s = float(parts[4])
                        except Exception:
                            pass
                    break
        return Entry(backend=backend, rows=rows, read_s=read_s, compute_s=compute_s, ok=True, input_rows=_count_input_rows(rows, None))
    except subprocess.TimeoutExpired:
        # Exceeded budget; mark as not ok so we won't escalate further for this backend
        return Entry(backend=backend, rows=rows, read_s=None, compute_s=None, ok=False)
    except Exception:
        # Any other failure (import problems, run error) is treated as not ok
        return Entry(backend=backend, rows=rows, read_s=None, compute_s=None, ok=False)


def fmt_fixed(headers: List[str], rows: List[List[str]]) -> List[str]:
    try:
        import sys as _sys
        from pathlib import Path as _Path

        _here = _Path(__file__).resolve()
        _sys.path.append(str(_here.parents[1]))  # scripts
        from utils import format_fixed as utils_format_fixed  # type: ignore

        return utils_format_fixed(headers, rows, right_align_from=2)
    except Exception:
        widths = [max(len(headers[i]), max((len(r[i]) for r in rows), default=0)) for i in range(len(headers))]

        def fmt_row(vals: List[str]) -> str:
            parts: List[str] = []
            for i, v in enumerate(vals):
                if i < 2:
                    parts.append(v.ljust(widths[i]))
                else:
                    parts.append(v.rjust(widths[i]))
            return "  ".join(parts)

        lines = [fmt_row(headers), "  ".join(("-" * w) for w in widths)]
        for r in rows:
            lines.append(fmt_row(r))
        return lines


def _build_row(entry: Entry, size: int) -> List[str]:
    rs = f"{entry.read_s:.4f}" if entry.read_s is not None else "-"
    cs = f"{entry.compute_s:.4f}" if entry.compute_s is not None else "-"
    ir = f"{entry.input_rows}" if entry.input_rows is not None else "-"
    ok = "yes" if entry.ok else "no"
    return [entry.backend, f"{size:.1e}", rs, cs, ir, ok]


def main():
    """CLI entrypoint.

    Parses the per-step budget, iterates backends and sizes in order-of-magnitude
    steps, runs each step with a timeout, and writes a fixed-width Markdown
    report to `reports/billion_row_om.md`.
    """

    parser = argparse.ArgumentParser(description="Order-of-magnitude BRC runner with per-step 3-minute cap")
    parser.add_argument("--budgets", type=float, default=DEFAULT_BUDGET_S)
    parser.add_argument("--data-glob-template", default=None)
    args = parser.parse_args()

    budget = float(args.budgets)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    headers = ["backend", "rows(sci)", "read_s", "compute_s", "input_rows", "ok"]
    rows: List[List[str]] = []

    for backend in Backends:
        for size in ORDERS:
            entry = _run_with_glob(backend, _parquet_glob(size), budget) if __import__("glob").glob(_parquet_glob(size)) else run_once(backend, size, budget)
            rows.append(_build_row(entry, size))
            if not entry.ok:
                break

    content = ["# Billion Row OM Runner", "", f"Generated at: {ts}", "", "```text", *fmt_fixed(headers, rows), "```", ""]
    OUT.write_text("\n".join(content))
    print("Wrote", OUT)


if __name__ == "__main__":
    sys.exit(main())
