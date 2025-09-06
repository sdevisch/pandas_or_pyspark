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
from unipandas.io import read_csv, read_parquet
from unipandas.frame import Frame
try:
    from .utils import (
        get_backend_version as utils_get_backend_version,
        used_cores_for_backend as utils_used_cores_for_backend,
        check_available as utils_check_available,
        format_fixed as utils_format_fixed,
        Backends as ALL_BACKENDS,
    )
except Exception:
    # Allow running as a standalone script
    import sys as _sys
    from pathlib import Path as _Path

    _here = _Path(__file__).resolve()
    # Add both 'scripts/brc' and 'scripts' to sys.path so we can import utils
    _sys.path.append(str(_here.parents[0]))  # scripts/brc
    _sys.path.append(str(_here.parents[1]))  # scripts
    from utils import (  # type: ignore
        get_backend_version as utils_get_backend_version,
        used_cores_for_backend as utils_used_cores_for_backend,
        check_available as utils_check_available,
        format_fixed as utils_format_fixed,
        Backends as ALL_BACKENDS,
    )

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
REPORTS = ROOT / "reports" / "brc"
REPORTS.mkdir(parents=True, exist_ok=True)
OUT = REPORTS / "billion_row_challenge.md"

Backends = ALL_BACKENDS


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
    return utils_get_backend_version(backend)


def _used_cores_for_backend(backend: str) -> Optional[int]:
    return utils_used_cores_for_backend(backend)


def check_available(backend: str) -> bool:
    return utils_check_available(backend)


def format_fixed(headers: List[str], rows: List[List[str]]) -> List[str]:
    return utils_format_fixed(headers, rows)


def main():
    parser = argparse.ArgumentParser(description="Billion Row Challenge (safe scaffold)")
    parser.add_argument("--rows-per-chunk", type=int, default=1_000_000, help="Rows per CSV chunk")
    parser.add_argument("--num-chunks", type=int, default=1, help="Number of chunks to generate")
    parser.add_argument("--operation", default="filter", choices=["filter", "groupby"], help="Operation to run")
    parser.add_argument("--data-glob", default=None, help="If set, use existing chunks (parquet or csv) matching this glob instead of generating")
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

        # Read all chunks, supporting parquet or csv
        t0 = time.perf_counter()
        frames: List[Frame] = []
        for p in chunks:
            if p.suffix.lower() == ".parquet":
                frames.append(read_parquet(str(p)))
            else:
                frames.append(read_csv(str(p)))
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


