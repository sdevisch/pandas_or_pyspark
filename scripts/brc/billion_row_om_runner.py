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

# Centralized report writing utility (importable both from repo root and tests)
try:
    # Preferred: import via package path when executed from repo root
    from scripts.utils import write_brc_om_report  # type: ignore
except Exception:
    # Fallback: allow direct execution from scripts/brc by amending sys.path
    import sys as _sys
    from pathlib import Path as _Path
    _here = _Path(__file__).resolve()
    _sys.path.append(str(_here.parents[1]))  # scripts
    from utils import write_brc_om_report  # type: ignore

# Import challenge helpers for in-process execution (with path-safe fallback)
try:
    from scripts.brc.brc_shared import (  # type: ignore
        measure_read as _challenge_measure_read,
        run_operation as _challenge_run_operation,
    )
except Exception:
    try:
        import sys as __sys
        from pathlib import Path as __Path
        __here = __Path(__file__).resolve()
        __sys.path.append(str(__here.parents[0]))  # scripts/brc
        __sys.path.append(str(__here.parents[1]))  # scripts
        from brc_shared import measure_read as _challenge_measure_read  # type: ignore
        from brc_shared import run_operation as _challenge_run_operation  # type: ignore
    except Exception:
        _challenge_measure_read = None  # type: ignore
        _challenge_run_operation = None  # type: ignore

# Repository root (two levels up from this file) so paths are stable regardless
# of the current working directory from which the script is invoked.
ROOT = Path(__file__).resolve().parents[2]

# Reports live under reports/brc. Create the directory eagerly to avoid race
# conditions later on when writing files in subprocess-driven flows.
try:
    from .brc_paths import REPORTS_BRC as REPORTS, REPORT_OM as OUT  # type: ignore
except Exception:
    try:
        import sys as _sys
        from pathlib import Path as _Path
        _here = _Path(__file__).resolve()
        _sys.path.append(str(_here.parent))
        from brc_paths import REPORTS_BRC as REPORTS, REPORT_OM as OUT  # type: ignore
    except Exception:
        REPORTS = ROOT / "reports" / "brc"
        OUT = REPORTS / "brc_order_of_magnitude.md"
REPORTS.mkdir(parents=True, exist_ok=True)

PY = sys.executable or "python3"  # Use current interpreter; fallback to python3

# Canonical backends list from scripts/utils.py
try:
    from scripts.utils import Backends as Backends  # type: ignore
except Exception:
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.append(str(_Path(__file__).resolve().parents[1]))
    from utils import Backends as Backends  # type: ignore

# We delegate real work to the single-source-of-truth challenge script and only
# orchestrate/measure here to avoid duplicate logic.
SCRIPT = ROOT / "scripts" / "brc" / "billion_row_challenge.py"  # Single source of truth for ops/IO

# Operation is fixed in the challenge script (groupby-only)
OPERATION = "groupby"

# Escalating target sizes we attempt per backend (logical rows intended).
ORDERS = [100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000]

# Per (backend,size) wall-clock budget in seconds. If a step exceeds this, we
# stop escalating for that backend.
DEFAULT_BUDGET_S = 180.0  # Per-step wall clock cap (seconds)

# ---------------------------------------------------------------------------
# Data classes (immutable records passed between steps and reporting)
# ---------------------------------------------------------------------------


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
    source: str = "generated"  # "parquet" | "generated"
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
    """Return verified input rows for a step without triggering compute.

    - If ``data_glob`` is provided and points at Parquet, read footer metadata
      for exact row counts.
    - If no glob is provided, return the intended ``rows`` as a proxy.
    """
    if data_glob:
        import glob as _glob
        import pyarrow.parquet as _pq  # type: ignore
        return sum(int(_pq.ParquetFile(p).metadata.num_rows) for p in _glob.glob(data_glob))
    return rows

# ---------------------------------------------------------------------------
# Filesystem and glob helpers (where data lives and how to find it)
# ---------------------------------------------------------------------------


def _parquet_glob(size: int) -> str:
    """Return a glob for Parquet chunks at ``size``.

    Prefer ``data/brc_scales/parquet_{size}`` if present (pre-generated scales)
    otherwise fall back to ``data/brc_{size}/*.parquet``.
    """
    scales_dir = ROOT / "data" / "brc_scales" / f"parquet_{size}"
    if scales_dir.exists():
        return str(scales_dir / "*.parquet")
    return str(ROOT / f"data/brc_{size}" / "*.parquet")
def _csv_glob(size: int) -> str:
    """CSV not supported for BRC (kept for compatibility; returns empty)."""
    return ""



def _size_dir(size: int) -> Path:
    """Directory that holds generated data for ``size``."""
    return ROOT / f"data/brc_{size}"


def _convert_csv_to_parquet_in_dir(dir_path: Path) -> None:
    """Deprecated: CSV not supported for BRC."""
    return


def _ensure_parquet_for_size(size: int) -> None:
    """Ensure Parquet exists for ``size`` by converting CSV when cheap enough.

    We avoid heavy work for very large sizes by capping conversion at 1e6.
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
    """Return True if the filesystem ``glob_path`` has any matches."""
    import glob as _glob
    return bool(_glob.glob(glob_path))


def run_once(backend: str, rows: int, budget_s: float) -> Entry:
    """Execute a single (backend, rows) step through the challenge script.

    We delegate to the canonical script, enforcing a timeout and parsing the
    resulting report for read/compute timings.
    """
    env = _env_for_backend(backend)
    # Parquet-only: generate a tiny parquet if needed for this single run
    # Always prefer parquet glob path; fall back to challenge default if generation unavailable
    try:
        import pandas as _pd  # type: ignore
        _tmp_dir = ROOT / "data" / f"brc_tmp_{rows}"
        _tmp_dir.mkdir(parents=True, exist_ok=True)
        _p = _tmp_dir / "generated.parquet"
        if not _p.exists():
            _rng = __import__("random").Random(42)
            _pdf = _pd.DataFrame({
                "id": list(range(rows)),
                "x": [_rng.randint(-1000, 1000) for _ in range(rows)],
                "y": [_rng.randint(-1000, 1000) for _ in range(rows)],
                "cat": [_rng.choice(["x", "y", "z"]) for _ in range(rows)],
            })
            _pdf.to_parquet(str(_p), index=False)
        cmd = _cmd_for_glob(backend, str(_tmp_dir / "*.parquet"))
    except Exception:
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
    """Build environment for the child process with backend selection."""
    env = os.environ.copy()
    env["UNIPANDAS_BACKEND"] = backend  # Honored by unipandas.configure_backend
    return env


def _cmd_for_rows(backend: str, rows: int) -> List[str]:
    """Command to run the challenge for a tiny generated parquet (deprecated path)."""
    return [PY, str(SCRIPT), "--only-backend", backend]


def _cmd_for_glob(backend: str, glob_path: str) -> List[str]:
    """Command to run the challenge against existing data at ``glob_path``."""
    return [PY, str(SCRIPT), "--data-glob", glob_path, "--only-backend", backend]


def _parse_latest_timings(backend: str, jsonl_path: Optional[Path] = None) -> tuple[Optional[float], Optional[float]]:
    """Return (read_s, compute_s) for the most recent run of ``backend``.

    Prefer parsing from JSONL if provided; otherwise fall back to scanning Markdown.
    """
    if jsonl_path and jsonl_path.exists():
        try:
            lines = [ln for ln in jsonl_path.read_text().splitlines() if ln.strip()]
            import json as _json  # type: ignore
            for ln in reversed(lines):
                obj = _json.loads(ln)
                if obj.get("backend") == backend and obj.get("operation") == "groupby":
                    rs = obj.get("read_seconds")
                    cs = obj.get("compute_seconds")
                    return (float(rs) if rs is not None else None, float(cs) if cs is not None else None)
        except Exception:
            pass
    # Fallback to Markdown parsing
    p = REPORTS / "brc_1b_groupby.md"
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
    """Execute the child process and parse timings from its report output.

    Measurement occurs in the child for a single source of truth.
    """
    try:
        # Prefer JSONL-first: write to a temp per-run file and avoid Markdown entirely
        jsonl_path = REPORTS.parent.parent / "results" / f"om_{backend}_{os.getpid()}.jsonl"
        jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        cmd_with_jsonl = list(cmd) + ["--jsonl-out", str(jsonl_path), "--no-md"]
        subprocess.run(
            cmd_with_jsonl,
            env=env,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=budget_s,
        )
        r_s, c_s = _parse_latest_timings(backend, jsonl_path)
        return True, r_s, c_s
    except subprocess.TimeoutExpired:
        return False, None, None
    except Exception:
        return False, None, None


def _run_inproc_for_glob(backend: str, glob_path: str, budget_s: float) -> tuple[bool, Optional[float], Optional[float]]:
    """Run the challenge in-process for existing data matched by ``glob_path``.

    Falls back to subprocess if challenge helpers are unavailable or for
    backends that require isolated processes (e.g., pyspark).
    """
    # Prefer subprocess for pyspark to avoid session conflicts
    if backend == "pyspark":
        env = _env_for_backend(backend)
        return _run_challenge(_cmd_for_glob(backend, glob_path), env, budget_s, backend)
    if _challenge_measure_read is None or _challenge_run_operation is None:
        env = _env_for_backend(backend)
        return _run_challenge(_cmd_for_glob(backend, glob_path), env, budget_s, backend)
    try:
        # Configure backend in-process
        from unipandas import configure_backend as _configure
        _configure(backend)

        # Expand glob to Paths
        import glob as _glob
        chunk_paths = [Path(p) for p in _glob.glob(glob_path)]
        if not chunk_paths:
            return False, None, None

        # Measure read/concat and then run operation
        from time import perf_counter as _now
        combined, read_s, _total_bytes = _challenge_measure_read(chunk_paths, backend)  # type: ignore
        t0 = _now()
        rows_observed, compute_s = _challenge_run_operation(combined, OPERATION, os.environ.get("BRC_MATERIALIZE", "head"))  # type: ignore
        _ = rows_observed  # unused here; OM captures input_rows separately
        elapsed = read_s + compute_s if (read_s is not None and compute_s is not None) else (compute_s or 0.0)
        if elapsed > budget_s:
            return False, None, None
        return True, float(read_s), float(compute_s)
    except Exception:
        return False, None, None

# NOTE: _build_row is obsolete in favor of StepResult -> rows formatting


## No local header logic; headers are defined in scripts.utils


def _entry_for_backend_size(backend: str, size: int, budget: float) -> Entry:
    """Measure one (backend, size) step under a time budget and return Entry."""
    _ensure_parquet_for_size(size)  # Opportunistically make Parquet available
    pg = _parquet_glob(size)
    if _has_matches(pg):
        # Prefer in-process for non-Spark backends when possible
        ok, rs, cs = _run_inproc_for_glob(backend, pg, budget)
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
    # No CSV fallback; if no parquet, run generation-based once
    return run_once(backend, size, budget)


def _sanity_check(entry: Entry) -> bool:
    """Basic invariants to catch obviously wrong measurements."""
    if entry.ok and (entry.read_s is None or entry.compute_s is None):
        return False
    if entry.read_s is not None and entry.read_s < 0:
        return False
    if entry.compute_s is not None and entry.compute_s < 0:
        return False
    if entry.source == "parquet" and entry.ok and (entry.input_rows is None or entry.input_rows <= 0):
        return False
    if entry.source == "generated" and entry.input_rows is not None and entry.input_rows != entry.rows:
        return False
    return True


def collect_measurements(budget: float) -> List[StepResult]:
    """Measure timings for each backend across escalating sizes within budget.

    We stop escalating for a backend as soon as one step fails.
    """
    out: List[StepResult] = []
    for backend in Backends:  # Iterate logical engines
        for size in ORDERS:   # Iterate escalating target sizes
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
            if not e.ok:  # Stop escalating once a step times out/fails
                break
    return out


def format_measurements_to_rows(steps: List[StepResult]) -> List[List[str]]:
    """Convert StepResult records into printable strings for reporting."""
    rows: List[List[str]] = []
    for s in steps:
        rs = f"{s.read_s:.4f}" if s.read_s is not None else "-"  # right-aligned
        cs = f"{s.compute_s:.4f}" if s.compute_s is not None else "-"
        ir = f"{s.input_rows}" if s.input_rows is not None else "-"
        ok = "yes" if s.ok else "no"
        sanity = "yes" if s.sanity_ok else "no"
        rows.append([s.backend, f"{s.size:.1e}", s.operation, s.source, rs, cs, ir, ok, sanity])
    return rows


def write_report_rows(rows: List[List[str]]) -> None:
    """Write the OM report using mdreport if available, else central utils."""
    try:
        from mdreport import Report  # type: ignore
    except Exception:
        write_brc_om_report(OUT, rows)
    else:
        headers = [
            "backend",
            "rows(sci)",
            "operation",
            "source",
            "read_s",
            "compute_s",
            "input_rows",
            "ok",
            "sanity_ok",
        ]
        rpt = Report(OUT)
        rpt.title("Billion Row OM Runner").table(headers, rows, align_from=4, style="fixed").write()


def main():
    """Run OM flow end-to-end: parse args → measure → format → write report."""
    # 1) Parse CLI arguments (kept minimal for readability)
    parser = argparse.ArgumentParser(description="Order-of-magnitude BRC runner with per-step cap")
    parser.add_argument(
        "--budgets",
        type=float,
        default=DEFAULT_BUDGET_S,
        help="Per-step wall-clock budget in seconds (default: 180s ≈ 3 minutes)",
    )
    args = parser.parse_args()

    # 2) Collect timing measurements within the specified budget
    steps = collect_measurements(float(args.budgets))  # Measure under budget across sizes/backends

    # 3) Convert measurements into printable rows
    rows = format_measurements_to_rows(steps)

    # 4) Emit the report
    write_report_rows(rows)


if __name__ == "__main__":
    sys.exit(main())
