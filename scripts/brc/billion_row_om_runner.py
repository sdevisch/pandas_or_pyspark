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
backend, rows, read_s, compute_s, input_rows, ok

Where is the “main work” done?
------------------------------
- The actual timing of read/compute happens inside the challenge script. We
  call it and then parse the generated Markdown report for numbers. See
  `_run_challenge` and `_parse_latest_timings`.
- Scale verification happens here by counting input rows via Parquet metadata
  or CSV line counts. See `_count_input_rows`.

Skepticism controls (to audit surprising results)
-------------------------------------------------
- Use the challenge with `--materialize count` to force full compute on lazy
  backends; compare timings to `head` materialization.
- Inspect `input_rows` to ensure we actually processed the intended number of
  rows.
- Prefer pre-generated Parquet data for large sizes to avoid regeneration and
  to provide exact row counts via footers.

Notes
-----
- We intentionally re-use the underlying script rather than duplicate logic,
  to keep a single source of truth for data generation and per-backend ops.
- We pass the backend selection via `UNIPANDAS_BACKEND`, which the library
  honors when configuring the current backend.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Repository root (two levels up from this file) so paths are stable regardless
# of the current working directory from which the script is invoked.
ROOT = Path(__file__).resolve().parents[2]

# Reports live under reports/brc. Create the directory eagerly to avoid race
# conditions later on when writing files in subprocess-driven flows.
REPORTS = ROOT / "reports" / "brc"
REPORTS.mkdir(parents=True, exist_ok=True)

# Destination report path for this runner.
OUT = REPORTS / "billion_row_om.md"

PY = sys.executable or "python3"  # Use current interpreter; fallback to python3

# We delegate real work to the single-source-of-truth challenge script and only
# orchestrate/measure here to avoid duplicate logic.
SCRIPT = ROOT / "scripts" / "brc" / "billion_row_challenge.py"

Backends = ["pandas", "dask", "pyspark", "polars", "duckdb"]  # Execution backends under test

# Operation performed by the challenge for OM runs (could be made CLI-configurable)
OPERATION = "filter"

# Escalating target sizes we attempt per backend (logical rows intended).
ORDERS = [100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000]

# Per (backend,size) wall-clock budget in seconds. If a step exceeds this, we
# stop escalating for that backend.
DEFAULT_BUDGET_S = 180.0


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
    source: str = "generated"  # "parquet" | "csv" | "generated"
    operation: str = OPERATION


@dataclass
class StepResult:
    """Single measured step for reporting (decoupled from rendering).

    backend: Which backend was executed (pandas/dask/pyspark/polars/duckdb)
    size: Logical row size attempted for the step
    read_s: Seconds spent reading/concatenating inputs (None if failed)
    compute_s: Seconds executing the operation (None if failed)
    input_rows: Verified input rows used (from Parquet/CSV), if available
    ok: Whether the step completed within the time budget
    """
    backend: str
    size: int
    read_s: Optional[float]
    compute_s: Optional[float]
    input_rows: Optional[int]
    ok: bool
    source: str
    operation: str
    sanity_ok: bool


def _count_input_rows(rows: int, data_glob: Optional[str]) -> Optional[int]:
    """Determine the number of input rows used for a step.

    If ``data_glob`` is provided, we try to read exact row counts from Parquet
    file metadata. If the files are CSV, we fall back to counting lines and
    subtracting the header. When no glob is provided, we return the nominal
    ``rows`` size as a proxy. This function forces no compute in backends.
    """
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
    """Return the default glob that points to Parquet chunks for a given size."""
    return str(ROOT / f"data/brc_{size}" / "*.parquet")
def _csv_glob(size: int) -> str:
    """Return the default glob that points to CSV chunks for a given size."""
    return str(ROOT / f"data/brc_{size}" / "*.csv")



def _size_dir(size: int) -> Path:
    """Directory for a given logical size."""
    return ROOT / f"data/brc_{size}"


def _convert_csv_to_parquet_in_dir(dir_path: Path) -> None:
    """Convert all CSV files in ``dir_path`` to Parquet side-by-side.

    Skips files where the corresponding .parquet already exists.
    """
    import glob as _glob
    import pandas as _pd  # type: ignore
    for c in _glob.glob(str(dir_path / "*.csv")):
        p_out = Path(c).with_suffix(".parquet")
        if p_out.exists():
            continue
        df = _pd.read_csv(c)
        df.to_parquet(p_out, index=False)


def _ensure_parquet_for_size(size: int) -> None:
    """Ensure Parquet chunks exist for ``size`` by converting existing CSV.

    To avoid heavy work unintentionally, only convert for sizes up to 1e6.
    """
    dir_path = _size_dir(size)
    if not dir_path.exists():
        return
    if any(dir_path.glob("*.parquet")):
        return
    if size > 1_000_000:
        return
    _convert_csv_to_parquet_in_dir(dir_path)




def _has_matches(glob_path: str) -> bool:
    """Return True if the filesystem glob has any matches.

    We encapsulate this to avoid non-obvious usage of __import__("glob").
    """
    import glob as _glob
    return bool(_glob.glob(glob_path))


def run_once(backend: str, rows: int, budget_s: float) -> Entry:
    """Run one (backend, size) step and parse read/compute timings.

    This delegates to the challenge script with a hard timeout, then parses the
    resulting report for the last (read_s, compute_s) values for ``backend``.
    """
    env = _env_for_backend(backend)
    cmd = _cmd_for_rows(backend, rows)
    ok, read_s, compute_s = _run_challenge(cmd, env, budget_s, backend)
    if not ok:
        return Entry(backend=backend, rows=rows, read_s=None, compute_s=None, ok=False)
    return Entry(
        backend=backend,
        rows=rows,
        read_s=read_s,
        compute_s=compute_s,
        ok=True,
        input_rows=_count_input_rows(rows, None),
        source="generated",
        operation=OPERATION,
    )


def _env_for_backend(backend: str) -> dict:
    """Build a subprocess environment that pins the backend to ``backend``."""
    env = os.environ.copy()
    env["UNIPANDAS_BACKEND"] = backend
    return env


def _cmd_for_rows(backend: str, rows: int) -> List[str]:
    """Create the challenge command to generate/run one chunk with ``rows``."""
    return [PY, str(SCRIPT), "--rows-per-chunk", str(rows), "--num-chunks", "1", "--operation", OPERATION, "--only-backend", backend]


def _cmd_for_glob(backend: str, glob_path: str) -> List[str]:
    """Create the challenge command to run against existing data at ``glob_path``."""
    return [PY, str(SCRIPT), "--data-glob", glob_path, "--operation", OPERATION, "--only-backend", backend]


def _parse_latest_timings(backend: str) -> tuple[Optional[float], Optional[float]]:
    """Parse the most recent (read_s, compute_s) for ``backend`` from report.

    We scan bottom-up to find the last line that starts with ``backend`` and
    read the timing columns. Returns (None, None) if not found.
    """
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
    """Execute the challenge subprocess and return (ok, read_s, compute_s).

    Timing occurs in the child process; we parse the challenge report to
    extract numbers. Keeping all measurement in one place helps avoid drift.
    """
    try:
        # Run with hard wall-clock cap; capture output for debugging if needed
        subprocess.run(
            cmd,
            env=env,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=budget_s,
        )
        r_s, c_s = _parse_latest_timings(backend)
        return True, r_s, c_s
    except subprocess.TimeoutExpired:
        return False, None, None
    except Exception:
        return False, None, None

def _build_row(entry: Entry, size: int) -> List[str]:
    """Format a single table row from an Entry and logical ``size`` value."""
    rs = f"{entry.read_s:.4f}" if entry.read_s is not None else "-"
    cs = f"{entry.compute_s:.4f}" if entry.compute_s is not None else "-"
    ir = f"{entry.input_rows}" if entry.input_rows is not None else "-"
    ok = "yes" if entry.ok else "no"
    return [entry.backend, f"{size:.1e}", entry.operation, entry.source, rs, cs, ir, ok]


def _default_headers() -> List[str]:
    """Headers for the OM report table (fixed-width formatting)."""
    return ["backend", "rows(sci)", "operation", "source", "read_s", "compute_s", "input_rows", "ok", "sanity_ok"]


def _entry_for_backend_size(backend: str, size: int, budget: float) -> Entry:
    """Return the measured Entry for (backend, size) under a time budget."""
    _ensure_parquet_for_size(size)
    pg = _parquet_glob(size)
    if _has_matches(pg):
        env = _env_for_backend(backend)
        ok, rs, cs = _run_challenge(_cmd_for_glob(backend, pg), env, budget, backend)
        return Entry(
            backend=backend,
            rows=size,
            read_s=rs,
            compute_s=cs,
            ok=bool(ok),
            input_rows=_count_input_rows(size, pg) if ok else None,
            source="parquet",
            operation=OPERATION,
        )
    cg = _csv_glob(size)
    if _has_matches(cg):
        env = _env_for_backend(backend)
        ok, rs, cs = _run_challenge(_cmd_for_glob(backend, cg), env, budget, backend)
        return Entry(
            backend=backend,
            rows=size,
            read_s=rs,
            compute_s=cs,
            ok=bool(ok),
            input_rows=_count_input_rows(size, cg) if ok else None,
            source="csv",
            operation=OPERATION,
        )
    return run_once(backend, size, budget)


def _sanity_check(entry: Entry) -> bool:
    """Basic validation to catch obviously wrong measurements."""
    if entry.ok and (entry.read_s is None or entry.compute_s is None):
        return False
    if entry.read_s is not None and entry.read_s < 0:
        return False
    if entry.compute_s is not None and entry.compute_s < 0:
        return False
    if entry.source in ("parquet", "csv") and entry.ok and (entry.input_rows is None or entry.input_rows <= 0):
        return False
    if entry.source == "generated" and entry.input_rows is not None and entry.input_rows != entry.rows:
        return False
    return True


def collect_measurements(budget: float) -> List[StepResult]:
    """Measure (read_s, compute_s) for each backend at increasing sizes.

    Respects the per-step time budget and stops escalation for a backend once a
    step fails. Returns a flat list of StepResult objects.
    """
    out: List[StepResult] = []
    for backend in Backends:
        for size in ORDERS:
            e = _entry_for_backend_size(backend, size, budget)
            out.append(
                StepResult(
                    backend,
                    size,
                    e.read_s,
                    e.compute_s,
                    e.input_rows,
                    e.ok,
                    e.source,
                    e.operation,
                    _sanity_check(e),
                )
            )
            if not e.ok:
                break
    return out


def format_measurements_to_rows(steps: List[StepResult]) -> List[List[str]]:
    """Convert StepResult measurements into string rows for reporting."""
    rows: List[List[str]] = []
    for s in steps:
        rs = f"{s.read_s:.4f}" if s.read_s is not None else "-"
        cs = f"{s.compute_s:.4f}" if s.compute_s is not None else "-"
        ir = f"{s.input_rows}" if s.input_rows is not None else "-"
        ok = "yes" if s.ok else "no"
        sanity = "yes" if s.sanity_ok else "no"
        rows.append([s.backend, f"{s.size:.1e}", s.operation, s.source, rs, cs, ir, ok, sanity])
    return rows


def write_report_rows(rows: List[List[str]]) -> None:
    """Write the OM report (fixed-width Markdown) using shared utilities.

    Falls back to a simple TSV-like fenced block if utilities are unavailable.
    """
    try:
        import sys as _sys
        from pathlib import Path as _Path
        _here = _Path(__file__).resolve()
        _sys.path.append(str(_here.parents[1]))  # scripts
        from utils import write_fixed_markdown as _write_md  # type: ignore
        _write_md(OUT, "Billion Row OM Runner", _default_headers(), rows, preface_lines=None, right_align_from=4)
    except Exception:
        # Fallback minimal TSV-like output
        lines = ["# Billion Row OM Runner", "", "```text", "\t".join(_default_headers())]
        for r in rows:
            lines.append("\t".join(str(x) for x in r))
        lines.extend(["```", ""])
        OUT.write_text("\n".join(lines))
        print("Wrote", OUT)


def main():
    """Run the OM runner and write a Markdown report.

    Steps:
    1) Parse the per-step time budget from CLI flags
    2) Measure performance per backend across escalating sizes (time boxed)
    3) Format measurements into fixed-width table rows for Markdown
    4) Write the report via shared utilities (with a safe fallback)
    """
    # 1) Parse CLI arguments (kept minimal for readability)
    parser = argparse.ArgumentParser(description="Order-of-magnitude BRC runner with per-step cap")
    parser.add_argument("--budgets", type=float, default=DEFAULT_BUDGET_S)
    args = parser.parse_args()

    # 2) Collect timing measurements within the specified budget
    steps = collect_measurements(float(args.budgets))  # Measure under budget across sizes/backends

    # 3) Convert measurements into printable rows
    rows = format_measurements_to_rows(steps)

    # 4) Emit the report
    write_report_rows(rows)


if __name__ == "__main__":
    sys.exit(main())
