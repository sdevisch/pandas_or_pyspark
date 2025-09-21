#!/usr/bin/env python3

from __future__ import annotations

"""Relational API demo across backends.

This script orchestrates dataset generation and timings, delegating Markdown
to `src/mdreport`. It avoids import-time side effects.
"""

import argparse
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from unipandas import configure_backend
from unipandas.io import read_csv
from unipandas.frame import Frame

ROOT = Path(__file__).resolve().parents[2]
import sys as _sys
if str(ROOT) not in _sys.path:
    _sys.path.insert(0, str(ROOT))
from scripts.utils import (
    Backends as ALL_BACKENDS,
    get_backend_version,
    used_cores_for_backend as utils_used_cores_for_backend,
    check_available as utils_check_available,
    format_fixed as utils_format_fixed,
)
DATA = ROOT / "data"

Backends = ALL_BACKENDS


def ensure_datasets(n: int = 2_000_000) -> Dict[str, Path]:
    """Ensure small relational CSVs exist; generate deterministically if missing."""
    DATA.mkdir(exist_ok=True)
    left = DATA / "rel_left.csv"
    right = DATA / "rel_right.csv"
    if left.exists() and right.exists():
        return {"left": left, "right": right}
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


def _used_cores_for_backend(backend: str) -> Optional[int]:
    return utils_used_cores_for_backend(backend)




def run_bench(datasets: Dict[str, Path]) -> List[Result]:
    results: List[Result] = []
    for backend in Backends:
        # Skip experimental backends that don't support DataFrame-like relational ops yet
        if backend in {"numpy", "numba"}:
            continue
        if not utils_check_available(backend):
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
    return utils_format_fixed(headers, rows)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run relational API demos and write Markdown.")
    p.add_argument("--md-out", default=str(ROOT / "reports" / "api_demo" / "relational_api_demo.md"))
    return p.parse_args()


def main() -> int:
    args = parse_args()
    ds = ensure_datasets()
    results = run_bench(ds)
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
    from mdreport.api_demo import render_relational_demos  # type: ignore

    render_relational_demos(args.md_out, headers, rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


