#!/usr/bin/env python3
"""Lightweight cross-backend benchmark for unipandas (refactored).

Goals:
- Keep functions short (≤ 7 statements) and highly readable
- Add docstrings and inline comments for non-obvious lines
- Preserve prior behavior and CLI while improving structure

The script measures time to read a small CSV in each backend and run
optional steps (assign, query, groupby), then materializes a bounded head
to time compute. Outputs a fixed-width table to console and optional
Markdown. The actual per-backend work remains in :func:`benchmark`.
"""
import argparse
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple
from datetime import datetime
import platform

from unipandas import configure_backend, read_csv
# Support running as a script or as a module
try:  # when invoked as a module: python -m api_demo.bench_backends
    from .utils import (
        Backends as ALL_BACKENDS,
        get_backend_version as utils_get_backend_version,
        check_available as utils_check_available,
        format_fixed as utils_format_fixed,
        used_cores_for_backend as utils_used_cores_for_backend,
    )
except Exception:  # when invoked as a script: python scripts/api_demo/bench_backends.py
    import sys
    from pathlib import Path

    # Add the scripts directory (which contains utils.py) to sys.path
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from utils import (  # type: ignore
        Backends as ALL_BACKENDS,
        get_backend_version as utils_get_backend_version,
        check_available as utils_check_available,
        format_fixed as utils_format_fixed,
        used_cores_for_backend as utils_used_cores_for_backend,
    )


Backends = ALL_BACKENDS  # Canonical list comes from scripts/utils.py


def get_backend_version(backend: str) -> Optional[str]:
    """Return version string for ``backend`` or None if unavailable."""
    return utils_get_backend_version(backend)


def check_available(backend: str) -> bool:
    """Return True if required modules for ``backend`` can be imported."""
    return utils_check_available(backend)


def try_configure(backend: str) -> bool:
    """Attempt to configure unipandas for ``backend``.

    Returns False with a concise message if the backend is unavailable or
    initialization fails, allowing the caller to skip gracefully.
    """
    try:
        if not check_available(backend):
            print(f"[skip] backend={backend}: required modules not available")
            return False
        configure_backend(backend)
        return True
    except Exception as e:
        print(f"[skip] backend={backend}: {e}")
        return False


@dataclass
class Result:
    """Single benchmark result for a backend.

    - backend: Backend name ('pandas', 'dask', 'pyspark', ...)
    - load_s: Seconds to read the CSV into a backend-native dataframe
    - compute_s: Seconds to compute and materialize output
    - input_rows: Total input rows processed
    - groups: Number of groups produced (if groupby used)
    - used_cores: Approximate parallelism used by the backend
    - version: Version string of the backend module, if detectable
    """
    backend: str
    load_s: float
    compute_s: float
    input_rows: Optional[int]
    groups: Optional[int]
    used_cores: Optional[int]
    version: Optional[str]


def _format_fixed_width_table(
    headers: List[str], rows: List[List[str]], right_align_from: int = 2
) -> List[str]:
    """Return a fixed-width table as list of strings for aligned printing."""
    widths = [
        max(len(headers[i]), max((len(r[i]) for r in rows), default=0)) for i in range(len(headers))
    ]

    def fmt_row(vals: List[str]) -> str:
        parts: List[str] = []
        for i, v in enumerate(vals):
            if i < right_align_from:
                parts.append(v.ljust(widths[i]))
            else:
                parts.append(v.rjust(widths[i]))
        return "  ".join(parts)

    lines: List[str] = [fmt_row(headers), "  ".join(("-" * w) for w in widths)]
    for row in rows:
        lines.append(fmt_row(row))
    return lines


def _used_cores_for_backend(backend: str) -> Optional[int]:
    """Delegate to shared utils so all backends, incl. numpy/numba, are handled consistently."""
    try:
        return utils_used_cores_for_backend(backend)
    except Exception:
        return None


def _count_rows_backend(df) -> int:
    """Count rows without collecting entire dataset."""
    try:
        import pandas as _pd  # type: ignore
        if isinstance(df, _pd.DataFrame):
            return int(len(df.index))
    except Exception:
        pass
    try:
        import dask.dataframe as _dd  # type: ignore
        from dask.dataframe import DataFrame as _DaskDF  # type: ignore
        if isinstance(df, _DaskDF):
            return int(df.shape[0].compute())
    except Exception:
        pass
    try:
        import pyspark.pandas as _ps  # type: ignore
        from pyspark.pandas.frame import DataFrame as _PsDF  # type: ignore
        if isinstance(df, _PsDF):
            return int(df.to_spark().count())
    except Exception:
        pass
    try:
        import polars as _pl  # type: ignore
        if isinstance(df, _pl.DataFrame):
            return int(df.height)
    except Exception:
        pass
    try:
        return int(len(df))
    except Exception:
        return 0


def benchmark(path: str, query: Optional[str], assign: bool, groupby: Optional[str]) -> List[Result]:
    """Run the end-to-end micro-benchmark for each backend.

    Steps per backend:
    1) configure backend
    2) read CSV
    3) optional assign/query/groupby
    4) materialize to pandas (bounded head) and time compute
    """
    results: List[Result] = []
    for backend in Backends:
        if not try_configure(backend):  # Skip unavailable/problematic backends
            continue

        t0 = time.perf_counter()
        df = read_csv(path)  # Backend-aware CSV reader
        t1 = time.perf_counter()

        out = df
        if assign:
            try:
                out = out.assign(c=lambda x: x["a"] + x["b"])  # type: ignore  # simple column add
            except Exception as e:
                print(f"[warn] assign skipped for backend={backend}: {e}")
        if query:
            try:
                out = out.query(query)  # boolean selection
            except Exception as e:
                print(f"[warn] query skipped for backend={backend}: {e}")
        if groupby:
            try:
                out = out.groupby(groupby).agg({groupby: "count"})  # tiny aggregation
            except Exception as e:
                print(f"[warn] groupby skipped for backend={backend}: {e}")

        t2 = time.perf_counter()
        # Count input rows (pre-op) for clarity
        input_rows = _count_rows_backend(df.to_backend()) if hasattr(df, "to_backend") else None
        # For compute timing, materialize with a bounded head but report groups when groupby is used
        groups = None
        if groupby:
            try:
                groups = _count_rows_backend(out.to_backend()) if hasattr(out, "to_backend") else None
            except Exception:
                groups = None
        _ = out.head(1000000000).to_pandas()
        t3 = time.perf_counter()

        used = _used_cores_for_backend(backend)
        ver = get_backend_version(backend)
        print(
            f"[info] backend={backend} version={ver} used_cores={used} available_cores={os.cpu_count()}"
        )

        results.append(
            Result(
                backend=backend,
                load_s=t1 - t0,
                compute_s=t3 - t2,
                input_rows=input_rows,
                groups=groups,
                used_cores=used,
                version=ver,
            )
        )
    return results


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments (short and readable)."""
    p = argparse.ArgumentParser(description="Benchmark unipandas across backends")
    p.add_argument("path", help="Path to CSV file to read")
    p.add_argument("--query", default=None, help="Optional pandas query string, e.g. 'a > 0'")
    p.add_argument("--assign", action="store_true", help="Add column c = a + b before compute")
    p.add_argument("--groupby", default="cat", help="Groupby column name to count (default: cat)")
    p.add_argument("--code-file", default=None, help="Optional path to the pandas code file processed")
    p.add_argument("--md-out", default=None, help="If set, write results as Markdown to this file")
    p.add_argument("--auto-rows", type=int, default=None, help="If set, generate a synthetic CSV with this many rows and use it")
    p.add_argument("--use-existing-1m", action="store_true", help="Use data/bench_1000000.csv if present")
    return p.parse_args()


def _availability() -> List[Dict[str, object]]:
    """Build backend availability list with versions."""
    info: List[Dict[str, object]] = []
    for name in Backends:
        info.append({"backend": name, "version": get_backend_version(name), "available": check_available(name)})
    return info


def _rows_for_console(results: List[Result], availability: List[Dict[str, object]]) -> List[List[str]]:
    """Build console/markdown rows in a consistent backend order."""
    by_backend = {r.backend: r for r in results}
    rows: List[List[str]] = []
    for name in Backends:
        r = by_backend.get(name)
        if r:
            used = r.used_cores if (r.used_cores and r.used_cores > 0) else os.cpu_count()
            rows.append([name, str(r.version), f"{r.load_s:.4f}", f"{r.compute_s:.4f}", str(r.input_rows), str(used)])
        else:
            ver = next((a["version"] for a in availability if a["backend"] == name), None)
            rows.append([name, str(ver), "-", "-", "-", "-"])
    return rows


def _md_sections(args: argparse.Namespace, availability: List[Dict[str, object]], rows: List[List[str]]) -> List[str]:
    """Compose Markdown sections (header, availability, results)."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    md: List[str] = ["# unipandas benchmark", "", "## Run context", ""]
    md += [f"- Data file: `{args.path}`", f"- Ran at: {ts}", f"- Python: `{platform.python_version()}` on `{platform.platform()}`"]
    if args.code_file:
        md.append(f"- Code file: `{args.code_file}`")
    md.append(f"- System available cores: {os.cpu_count()}")
    md.append(f"- Args: assign={args.assign}, query={args.query!r}, groupby={args.groupby!r}")
    md += ["", "## Backend availability", "", "| backend | version | status |", "|---|---|---|"]
    for item in availability:
        md.append(f"| {item['backend']} | {item['version']} | {'available' if item['available'] else 'unavailable'} |")
    md += ["", "## Results (seconds)", "", "```text"]
    headers = ["backend", "version", "load_s", "compute_s", "input_rows", "used_cores"]
    for line in _format_fixed_width_table(headers, rows):
        md.append(line)
    md.append("```")
    return md


def main() -> int:
    """Entry point orchestrating parse → run → print → optional markdown."""
    args = _parse_args()
    # Optionally generate a synthetic dataset with requested row count
    if args.use_existing_1m:
        from pathlib import Path as _Path
        candidate = _Path(__file__).resolve().parents[2] / "data" / "bench_1000000.csv"
        if candidate.exists():
            args.path = str(candidate)
    elif args.auto_rows:
        from pathlib import Path as _Path
        import csv as _csv, random as _random
        data_dir = _Path(__file__).resolve().parents[2] / "data"
        data_dir.mkdir(exist_ok=True)
        gen_path = data_dir / f"bench_{args.auto_rows}.csv"
        if not gen_path.exists():
            _random.seed(42)
            with gen_path.open("w", newline="") as f:
                w = _csv.writer(f)
                w.writerow(["a", "b", "cat"])  # header
                for _ in range(int(args.auto_rows)):
                    w.writerow([
                        _random.randint(-1000, 1000),
                        _random.randint(-1000, 1000),
                        _random.choice(["x", "y", "z"]),
                    ])
        args.path = str(gen_path)
    print("Backends to try:", Backends)
    print("Availability:")
    avail = _availability()
    for item in avail:
        status = "available" if item["available"] else "unavailable"
        print(f"- {item['backend']}: {status} (version={item['version']})")
    results = benchmark(args.path, query=args.query, assign=args.assign, groupby=args.groupby)
    if not results:
        print("No backends available.")
        return 1
    print("\nRun context:")
    print(f"- Data file: {args.path}")
    if args.code_file:
        print(f"- Code file: {args.code_file}")
    print(f"- System available cores: {os.cpu_count()}")
    print("\nResults (seconds):")
    rows = _rows_for_console(results, avail)
    for line in _format_fixed_width_table(["backend", "version", "load_s", "compute_s", "rows", "used_cores"], rows):
        print(line)
    if args.md_out:
        # Prefer mdreport for consistency; fallback to existing behavior
        try:
            from mdreport import Report, system_info  # type: ignore
        except Exception:
            # fallback to previous content assembly
            content = "\n".join(_md_sections(args, avail, rows)) + "\n"
            with open(args.md_out, "w") as f:
                f.write(content)
            print(f"\nWrote Markdown results to {args.md_out}")
        else:
            rpt = Report(args.md_out)
            rpt.title("unipandas benchmark").preface([
                "## Run context",
                f"- Data file: `{args.path}`",
                f"- Ran at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                *system_info(),
                f"- Args: assign={args.assign}, query={args.query!r}, groupby={args.groupby!r}",
                "",
                "## Backend availability",
                "",
                "| backend | version | status |",
                "|---|---|---|",
                *[f"| {it['backend']} | {it['version']} | {'available' if it['available'] else 'unavailable'} |" for it in avail],
                "",
                "## Results (seconds)",
                "",
            ])
            rpt.table(["backend", "version", "load_s", "compute_s", "input_rows", "used_cores"], rows, align_from=2, style="fixed").write()
            print(f"\nWrote Markdown results to {args.md_out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())


