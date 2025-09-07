#!/usr/bin/env python3
"""Lightweight cross-backend benchmark for unipandas.

This script measures the time to:
1) Load a small CSV into each configured backend
2) Optionally perform an assign, a query, and a groupby
3) Materialize a bounded head() for fair compute comparison

It outputs both a nicely aligned console table and, if requested, a
Markdown report with run context and results.
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
    - rows: Number of rows in the materialized pandas output (if applicable)
    - used_cores: Approximate parallelism used by the backend
    - version: Version string of the backend module, if detectable
    """
    backend: str
    load_s: float
    compute_s: float
    rows: Optional[int]
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
    try:
        if backend == "pandas":
            return 1
        if backend == "dask":
            try:
                from distributed import get_client  # type: ignore

                try:
                    client = get_client()
                    nthreads = getattr(client, "nthreads", None)
                    if isinstance(nthreads, dict):
                        return sum(int(v) for v in nthreads.values())
                except Exception:
                    pass
            except Exception:
                pass
            return os.cpu_count() or None
        if backend == "pyspark":
            try:
                from pyspark.sql import SparkSession  # type: ignore

                active = SparkSession.getActiveSession()
                if active is not None:
                    sc = active.sparkContext
                    return int(getattr(sc, "defaultParallelism", None) or 0) or None
            except Exception:
                pass
            return None  # Spark parallelism is environment-dependent
    except Exception:
        return None
    return None


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
        # Use a very large head bound to cap transfer without affecting timings
        pdf = out.head(1000000000).to_pandas()
        rows = len(pdf.index) if hasattr(pdf, "index") else None
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
                rows=rows,
                used_cores=used,
                version=ver,
            )
        )
    return results


def main():
    parser = argparse.ArgumentParser(description="Benchmark unipandas across backends")
    parser.add_argument("path", help="Path to CSV file to read")
    parser.add_argument("--query", default=None, help="Optional pandas query string, e.g. 'a > 0' ")
    parser.add_argument("--assign", action="store_true", help="Add column c = a + b before compute")
    parser.add_argument("--groupby", default=None, help="Optional groupby column name to count")
    parser.add_argument("--code-file", default=None, help="Optional path to the pandas code file processed")
    parser.add_argument("--md-out", default=None, help="If set, write results as Markdown to this file")
    args = parser.parse_args()

    print("Backends to try:", Backends)
    print("Availability:")
    availability = []
    for name in Backends:
        ver = get_backend_version(name)
        ok = check_available(name)
        availability.append({"backend": name, "version": ver, "available": ok})
        status = "available" if ok else "unavailable"
        print(f"- {name}: {status} (version={ver})")

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
    headers = ["backend", "version", "load_s", "compute_s", "rows", "used_cores"]
    result_by_backend = {r.backend: r for r in results}
    rows_console: List[List[str]] = []
    for name in Backends:
        r = result_by_backend.get(name)
        if r is not None:
            rows_console.append(
                [
                    r.backend,
                    str(r.version),
                    f"{r.load_s:.4f}",
                    f"{r.compute_s:.4f}",
                    str(r.rows),
                    str(r.used_cores),
                ]
            )
        else:
            ver = next((a["version"] for a in availability if a["backend"] == name), None)
            rows_console.append([name, str(ver), "-", "-", "-", "-"])

    for line in _format_fixed_width_table(headers, rows_console):
        print(line)

    if args.md_out:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        md_lines: List[str] = []
        md_lines.append(f"# unipandas benchmark")
        md_lines.append("")
        md_lines.append("## Run context")
        md_lines.append("")
        md_lines.append(f"- Data file: `{args.path}`")
        md_lines.append(f"- Ran at: {ts}")
        md_lines.append(f"- Python: `{platform.python_version()}` on `{platform.platform()}`")
        if args.code_file:
            md_lines.append(f"- Code file: `{args.code_file}`")
        md_lines.append(f"- System available cores: {os.cpu_count()}")
        md_lines.append(
            f"- Args: assign={args.assign}, query={args.query!r}, groupby={args.groupby!r}"
        )
        md_lines.append("")
        md_lines.append("## Backend availability")
        md_lines.append("")
        md_lines.append("| backend | version | status |")
        md_lines.append("|---|---|---|")
        for item in availability:
            md_lines.append(
                f"| {item['backend']} | {item['version']} | {'available' if item['available'] else 'unavailable'} |"
            )
        md_lines.append("")
        md_lines.append("## Results (seconds)")
        md_lines.append("")
        # Use only fixed-width table for alignment
        md_lines.append("```text")
        headers = ["backend", "version", "load_s", "compute_s", "rows", "used_cores"]
        result_by_backend = {r.backend: r for r in results}
        rows_console: List[List[str]] = []
        for name in Backends:
            r = result_by_backend.get(name)
            if r is not None:
                rows_console.append([
                    r.backend,
                    str(r.version),
                    f"{r.load_s:.4f}",
                    f"{r.compute_s:.4f}",
                    str(r.rows),
                    str(r.used_cores),
                ])
            else:
                ver = next((a["version"] for a in availability if a["backend"] == name), None)
                rows_console.append([name, str(ver), "-", "-", "-", "-"])
        for line in _format_fixed_width_table(headers, rows_console):
            md_lines.append(line)
        md_lines.append("```")
        md_content = "\n".join(md_lines) + "\n"
        with open(args.md_out, "w") as f:
            f.write(md_content)
        print(f"\nWrote Markdown results to {args.md_out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())


