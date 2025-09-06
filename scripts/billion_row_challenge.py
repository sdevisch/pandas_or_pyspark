#!/usr/bin/env python3
"""
Billion Row Challenge (BRC) scaffold
------------------------------------

Generates chunked CSV data and runs a simple operation (filter or groupby)
across whichever backend is selected via `UNIPANDAS_BACKEND` (pandas, dask,
or pandas-on-Spark). Results are written as a fixed-width Markdown table
under `reports/billion_row_challenge.md`.

Design notes
------------
- Data generation is chunked to allow scaling up without holding everything in
  memory; each chunk is an independent CSV file.
- Concatenation uses backend-native concat where applicable to avoid eager
  collection into local memory.
- We materialize a `head` to pandas to ensure compute happens for lazy engines.
  For huge outputs, `head` limits transfer size to the driver.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import glob
from typing import List, Optional

from unipandas import configure_backend
from unipandas.io import read_csv
from unipandas.frame import Frame

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
REPORTS = ROOT / "reports"
REPORTS.mkdir(exist_ok=True)
OUT = REPORTS / "billion_row_challenge.md"

Backends = ["pandas", "dask", "pyspark"]


def ensure_chunks(rows_per_chunk: int, num_chunks: int, seed: int = 123) -> List[Path]:
    DATA.mkdir(exist_ok=True)
    paths: List[Path] = []
    import csv
    import random

    random.seed(seed)
    for i in range(num_chunks):
        p = DATA / f"brc_{i:04d}.csv"
        if not p.exists():
            with p.open("w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["id", "x", "y", "cat"])
                base = i * rows_per_chunk
                for j in range(rows_per_chunk):
                    rid = base + j
                    w.writerow([rid, random.randint(-1000, 1000), random.randint(-1000, 1000), random.choice(["x", "y", "z"])])
        paths.append(p)
    return paths


def existing_chunks(glob_pattern: str) -> List[Path]:
    """Return existing chunk paths matching the provided glob pattern."""
    return [Path(p) for p in glob.glob(glob_pattern)]


@dataclass
class Result:
    backend: str
    op: str
    read_s: float
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


def format_fixed(headers: List[str], rows: List[List[str]]) -> List[str]:
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
    parser = argparse.ArgumentParser(description="Billion Row Challenge (safe scaffold)")
    parser.add_argument("--rows-per-chunk", type=int, default=1_000_000, help="Rows per CSV chunk")
    parser.add_argument("--num-chunks", type=int, default=1, help="Number of chunks to generate")
    parser.add_argument("--operation", default="filter", choices=["filter", "groupby"], help="Operation to run")
    parser.add_argument("--data-glob", default=None, help="If set, use existing CSV chunks matching this glob instead of generating")
    args = parser.parse_args()

    if args.data_glob:
        chunks = existing_chunks(args.data_glob)
        if not chunks:
            raise SystemExit(f"No files matched --data-glob '{args.data_glob}'")
    else:
        chunks = ensure_chunks(args.rows_per_chunk, args.num_chunks)

    results: List[Result] = []

    for backend in Backends:
        if not check_available(backend):
            continue
        configure_backend(backend)

        # Read all chunks via read_csv (glob-like)
        t0 = time.perf_counter()
        frames = [read_csv(str(p)) for p in chunks]
        # simple concat with backend-specific tooling
        if backend == "pyspark":
            import pyspark.pandas as ps  # type: ignore

            combined = Frame(ps.concat([f.to_backend() for f in frames]))
        elif backend == "dask":
            import dask.dataframe as dd  # type: ignore

            combined = Frame(dd.concat([f.to_backend() for f in frames]))
        else:
            import pandas as pd  # type: ignore

            combined = Frame(pd.concat([f.to_backend() for f in frames]))
        t1 = time.perf_counter()

        used = _used_cores_for_backend(backend)
        ver = get_backend_version(backend)

        # Run op
        t2 = time.perf_counter()
        if args.operation == "filter":
            out = combined.query("x > 0 and y < 0")
        else:
            out = combined.groupby("cat").agg({"x": "sum", "y": "mean"})
        pdf = out.head(10_000_000).to_pandas()
        rows = len(pdf.index) if hasattr(pdf, "index") else None
        t3 = time.perf_counter()

        results.append(
            Result(
                backend=backend,
                op=args.operation,
                read_s=t1 - t0,
                compute_s=t3 - t2,
                rows=rows,
                used_cores=used,
                version=ver,
            )
        )

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    headers = ["backend", "version", "op", "read_s", "compute_s", "rows", "used_cores"]
    rows_out: List[List[str]] = [
        [
            r.backend,
            str(r.version),
            r.op,
            f"{r.read_s:.4f}",
            f"{r.compute_s:.4f}",
            str(r.rows),
            str(r.used_cores),
        ]
        for r in results
    ]
    lines = ["# Billion Row Challenge (scaffold)", "", f"Generated at: {ts}", "", "```text", *format_fixed(headers, rows_out), "```", ""]
    OUT.write_text("\n".join(lines))
    print("Wrote", OUT)


if __name__ == "__main__":
    sys.exit(main())


