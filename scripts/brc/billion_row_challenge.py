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
import os as _os

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


def _chunks_out_dir(rows_per_chunk: int) -> Path:
    DATA.mkdir(exist_ok=True)
    out = DATA / f"brc_{rows_per_chunk}"
    out.mkdir(exist_ok=True)
    return out


def _write_rows_csv(path: Path, rows: int, base: int, rnd) -> None:
    import csv
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "x", "y", "cat"])
        for j in range(rows):
            w.writerow([base + j, rnd.randint(-1000, 1000), rnd.randint(-1000, 1000), rnd.choice(["x", "y", "z"])])


def _maybe_generate_chunk(out_dir: Path, rows_per_chunk: int, i: int, rnd) -> Path:
    p = out_dir / f"brc_{rows_per_chunk}_{i:04d}.csv"
    if not p.exists():
        _write_rows_csv(p, rows_per_chunk, i * rows_per_chunk, rnd)
    return p


def ensure_chunks(rows_per_chunk: int, num_chunks: int, seed: int = 123) -> List[Path]:
    out_dir = _chunks_out_dir(rows_per_chunk)
    rnd = __import__("random").Random(seed)
    return [_maybe_generate_chunk(out_dir, rows_per_chunk, i, rnd) for i in range(num_chunks)]


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


def parse_arguments():
    p = argparse.ArgumentParser(description="Billion Row Challenge (safe scaffold)")
    args_list = [
        ("--rows-per-chunk", dict(type=int, default=1_000_000)),
        ("--num-chunks", dict(type=int, default=1)),
        ("--operation", dict(default="filter", choices=["filter", "groupby"])),
        ("--data-glob", dict(default=None)),
        ("--only-backend", dict(default=None)),
        ("--md-out", dict(default=None)),
    ]
    for name, kw in args_list:
        p.add_argument(name, **kw)
    return p.parse_args()


def resolve_chunks(args) -> List[Path]:
    if args.data_glob:
        paths = existing_chunks(args.data_glob)
        if not paths:
            raise SystemExit(f"No files matched --data-glob '{args.data_glob}'")
        return paths
    return ensure_chunks(args.rows_per_chunk, args.num_chunks)


def read_frames_for_backend(chunks: List[Path], backend: str) -> List[Frame]:
    frames: List[Frame] = []
    for p in chunks:
        if p.suffix.lower() == ".parquet":
            frames.append(read_parquet(str(p)))
        else:
            frames.append(read_csv(str(p)))
    return frames


def concat_frames(frames: List[Frame], backend: str) -> Frame:
    if backend == "pyspark":
        import pyspark.pandas as ps  # type: ignore
        return Frame(ps.concat([f.to_backend() for f in frames]))
    if backend == "dask":
        import dask.dataframe as dd  # type: ignore
        return Frame(dd.concat([f.to_backend() for f in frames]))
    import pandas as pd  # type: ignore
    return Frame(pd.concat([f.to_backend() for f in frames]))


def measure_read(chunks: List[Path], backend: str) -> tuple[Frame, float, int]:
    t0 = time.perf_counter()
    frames = read_frames_for_backend(chunks, backend)
    combined = concat_frames(frames, backend)
    t1 = time.perf_counter()
    total_bytes = sum((p.stat().st_size for p in chunks if p.exists()), 0)
    return combined, t1 - t0, total_bytes


def run_operation(combined: Frame, op: str) -> tuple[int, float]:
    t2 = time.perf_counter()
    out = combined.query("x > 0 and y < 0") if op == "filter" else combined.groupby("cat").agg({"x": "sum", "y": "mean"})
    pdf = out.head(10_000_000).to_pandas()
    rows = len(pdf.index) if hasattr(pdf, "index") else 0
    t3 = time.perf_counter()
    return rows, t3 - t2


def run_backend(backend: str, chunks: List[Path], op: str) -> Result:
    configure_backend(backend)
    combined, read_s, _ = measure_read(chunks, backend)
    used = _used_cores_for_backend(backend)
    ver = get_backend_version(backend)
    rows, compute_s = run_operation(combined, op)
    return Result(backend=backend, op=op, read_s=read_s, compute_s=compute_s, rows=rows, used_cores=used, version=ver)


def choose_backends(only_backend: Optional[str]) -> List[str]:
    if only_backend:
        return [only_backend]
    return Backends


def build_rows(results: List[Result]) -> List[List[str]]:
    rows: List[List[str]] = []
    for r in results:
        rows.append([r.backend, str(r.version), r.op, f"{r.read_s:.4f}", f"{r.compute_s:.4f}", str(r.rows), str(r.used_cores)])
    return rows


def write_report(chunks: List[Path], results: List[Result], md_out: Optional[str]) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    headers = ["backend", "version", "op", "read_s", "compute_s", "rows", "used_cores"]
    lines = [
        "# Billion Row Challenge (scaffold)",
        "",
        f"Generated at: {ts}",
        "",
        f"- num_chunks: {len(chunks)}",
        f"- total_bytes: {sum((p.stat().st_size for p in chunks if p.exists()), 0)}",
        "",
        "```text",
        *format_fixed(headers, build_rows(results)),
        "```",
        "",
    ]
    out_path = Path(md_out) if md_out else OUT
    out_path.write_text("\n".join(lines))
    print("Wrote", out_path)

def main():
    args = parse_arguments()
    chunks = resolve_chunks(args)
    results: List[Result] = []
    backends_to_run = [args.only_backend] if args.only_backend else Backends
    for backend in backends_to_run:
        if not check_available(backend):
            continue
        results.append(run_backend(backend, chunks, args.operation))
    write_report(chunks, results, args.md_out)


if __name__ == "__main__":
    sys.exit(main())


