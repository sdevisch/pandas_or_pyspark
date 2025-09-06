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


def try_configure(backend: str) -> bool:
    try:
        if backend == "pandas":
            __import__("pandas")
        elif backend == "dask":
            __import__("dask.dataframe")
        elif backend == "pyspark":
            __import__("pyspark.pandas")
        else:
            print(f"[skip] backend={backend}: unknown backend")
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
            # assume columns a and b exist, or skip safely
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

        # force compute and measure
        t2 = time.perf_counter()
        pdf = out.head(1000000000).to_pandas()  # head with large n to trigger compute across backends
        rows = len(pdf.index) if hasattr(pdf, "index") else None
        t3 = time.perf_counter()

        results.append(Result(backend, load_s=t1 - t0, compute_s=t3 - t2, rows=rows))
    return results


def main():
    parser = argparse.ArgumentParser(description="Benchmark unipandas across backends")
    parser.add_argument("path", help="Path to CSV file to read")
    parser.add_argument("--query", default=None, help="Optional pandas query string, e.g. 'a > 0' ")
    parser.add_argument("--assign", action="store_true", help="Add column c = a + b before compute")
    parser.add_argument("--groupby", default=None, help="Optional groupby column name to count")
    parser.add_argument("--md-out", default=None, help="If set, write results as Markdown to this file")
    args = parser.parse_args()

    print("Backends to try:", Backends)
    results = benchmark(args.path, query=args.query, assign=args.assign, groupby=args.groupby)

    if not results:
        print("No backends available.")
        return 1

    print("\nResults (seconds):")
    print("backend\tload_s\tcompute_s\trows")
    for r in results:
        print(f"{r.backend}\t{r.load_s:.4f}\t{r.compute_s:.4f}\t{r.rows}")

    if args.md_out:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        md_lines: List[str] = []
        md_lines.append(f"# unipandas benchmark")
        md_lines.append("")
        md_lines.append(f"- Path: `{args.path}`")
        md_lines.append(f"- Ran at: {ts}")
        md_lines.append(f"- Python: `{platform.python_version()}` on `{platform.platform()}`")
        md_lines.append(f"- Args: assign={args.assign}, query={args.query!r}, groupby={args.groupby!r}")
        md_lines.append("")
        md_lines.append("| backend | load_s | compute_s | rows |")
        md_lines.append("|---|---:|---:|---:|")
        for r in results:
            md_lines.append(f"| {r.backend} | {r.load_s:.4f} | {r.compute_s:.4f} | {r.rows} |")
        md_content = "\n".join(md_lines) + "\n"
        with open(args.md_out, "w") as f:
            f.write(md_content)
        print(f"\nWrote Markdown results to {args.md_out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())


