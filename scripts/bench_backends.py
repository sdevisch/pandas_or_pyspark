#!/usr/bin/env python3
import argparse
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

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
    return 0


if __name__ == "__main__":
    sys.exit(main())


