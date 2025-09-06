#!/usr/bin/env python3


import argparse
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple
from datetime import datetime
import platform

from unipandas import configure_backend, read_csv


Backends = ["pandas", "dask", "pyspark"]


def get_backend_version(backend: str) -> Optional[str]:
    try:
        if backend == "pandas":
            import pandas as pd  # type: ignore

            return getattr(pd, "__version__", None)
        if backend == "dask":
            import dask  # type: ignore

            return getattr(dask, "__version__", None)
        if backend == "pyspark":
            import pyspark  # type: ignore

            return getattr(pyspark, "__version__", None)
    except Exception:
        return None
    return None


def check_available(backend: str) -> bool:
    try:
        if backend == "pandas":
            __import__("pandas")
        elif backend == "dask":
            __import__("dask.dataframe")
        elif backend == "pyspark":
            __import__("pyspark.pandas")
        else:
            return False
        return True
    except Exception:
        return False


def try_configure(backend: str) -> bool:
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
    backend: str
    load_s: float
    compute_s: float
    rows: Optional[int]
    used_cores: Optional[int]
    version: Optional[str]


def _format_fixed_width_table(
    headers: List[str], rows: List[List[str]], right_align_from: int = 2
) -> List[str]:
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
            return None
    except Exception:
        return None
    return None


def benchmark(path: str, query: Optional[str], assign: bool, groupby: Optional[str]) -> List[Result]:
    results: List[Result] = []
    for backend in Backends:
        if not try_configure(backend):
            continue

        t0 = time.perf_counter()
        df = read_csv(path)
        t1 = time.perf_counter()

        out = df
        if assign:
            try:
                out = out.assign(c=lambda x: x["a"] + x["b"])  # type: ignore
            except Exception as e:
                print(f"[warn] assign skipped for backend={backend}: {e}")
        if query:
            try:
                out = out.query(query)
            except Exception as e:
                print(f"[warn] query skipped for backend={backend}: {e}")
        if groupby:
            try:
                out = out.groupby(groupby).agg({groupby: "count"})
            except Exception as e:
                print(f"[warn] groupby skipped for backend={backend}: {e}")

        t2 = time.perf_counter()
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


