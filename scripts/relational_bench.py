#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from unipandas import configure_backend
from unipandas.io import read_csv
from unipandas.frame import Frame

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
REPORTS = ROOT / "reports"
REPORTS.mkdir(exist_ok=True)
OUT = REPORTS / "relational_benchmark.md"

Backends = ["pandas", "dask", "pyspark"]


def ensure_datasets(n: int = 2_000_000) -> Dict[str, Path]:
    DATA.mkdir(exist_ok=True)
    left = DATA / "rel_left.csv"
    right = DATA / "rel_right.csv"
    if not left.exists() or not right.exists():
        import csv
        import random

        random.seed(123)
        # left: id, key, v1
        with left.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["id", "key", "v1"])
            for i in range(n):
                w.writerow([i, i % 50000, random.randint(-1000, 1000)])
        # right: key, v2
        with right.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["key", "v2"])
            for k in range(50000):
                w.writerow([k, random.randint(-1000, 1000)])
    return {"left": left, "right": right}


@dataclass
class Result:
    backend: str
    op: str
    load_s: float
    compute_s: float
    rows: Optional[int]
    used_cores: Optional[int]
    version: Optional[str]


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


def run_bench(datasets: Dict[str, Path]) -> List[Result]:
    results: List[Result] = []
    for backend in Backends:
        if not check_available(backend):
            continue
        configure_backend(backend)

        # Load two dataframes
        t0 = time.perf_counter()
        left = read_csv(str(datasets["left"]))
        right = read_csv(str(datasets["right"]))
        t1 = time.perf_counter()

        ver = get_backend_version(backend)
        used = _used_cores_for_backend(backend)

        # Join on key
        t2 = time.perf_counter()
        joined = left.merge(right, on="key")
        # Force compute and count rows
        jpdf = joined.head(10_000_000).to_pandas()
        jrows = len(jpdf.index) if hasattr(jpdf, "index") else None
        t3 = time.perf_counter()
        results.append(
            Result(backend=backend, op="join", load_s=t1 - t0, compute_s=t3 - t2, rows=jrows, used_cores=used, version=ver)
        )

        # Concat: union two slices
        t4 = time.perf_counter()
        # Create a second piece by selecting alternating keys
        piece1 = left.query("key % 2 == 0")
        piece2 = left.query("key % 2 == 1")
        if backend == "pyspark":
            import pyspark.pandas as ps  # type: ignore

            concatted = Frame(ps.concat([piece1.to_backend(), piece2.to_backend()]))
        elif backend == "dask":
            import dask.dataframe as dd  # type: ignore

            concatted = Frame(dd.concat([piece1.to_backend(), piece2.to_backend()]))
        else:
            import pandas as pd  # type: ignore

            concatted = Frame(pd.concat([piece1.to_backend(), piece2.to_backend()]))
        cpdf = concatted.head(10_000_000).to_pandas()
        crows = len(cpdf.index) if hasattr(cpdf, "index") else None
        t5 = time.perf_counter()
        results.append(
            Result(backend=backend, op="concat", load_s=0.0, compute_s=t5 - t4, rows=crows, used_cores=used, version=ver)
        )
    return results


def fmt_fixed(headers: List[str], rows: List[List[str]]) -> List[str]:
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


def main():
    ds = ensure_datasets()
    results = run_bench(ds)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    headers = ["backend", "version", "op", "load_s", "compute_s", "rows", "used_cores"]
    rows: List[List[str]] = [
        [
            r.backend,
            str(r.version),
            r.op,
            f"{r.load_s:.4f}",
            f"{r.compute_s:.4f}",
            str(r.rows),
            str(r.used_cores),
        ]
        for r in results
    ]
    content = [
        "# Relational benchmarks",
        "",
        f"Generated at: {ts}",
        "",
        "```text",
        *fmt_fixed(headers, rows),
        "```",
        "",
    ]
    OUT.write_text("\n".join(content))
    print("Wrote", OUT)


if __name__ == "__main__":
    sys.exit(main())


