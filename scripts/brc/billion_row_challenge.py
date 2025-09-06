#!/usr/bin/env python=
"""Billion Row Challenge (BRC) scaffold.

This script provides a safe, repeatable harness for running a small set of
pandas-like operations across multiple data processing backends. It focuses on
two core operations — a boolean filter and a simple groupby/aggregation — and
logs the time to read/concatenate input chunks as well as to compute the
operation. Results are written as a fixed-width table for alignment in
Markdown.

Key properties:
- Data generation is chunked and size-specific (by rows-per-chunk). This avoids
  loading all rows into memory and prevents accidentally reusing small files
  for large sizes.
- Reading uses backend-appropriate IO and concatenation, but we always expose a
  unified API through `unipandas.Frame`.
- For lazy engines (e.g., Dask, pandas-on-Spark), we materialize a `head(...)`
  to force execution while limiting transfer size to the driver.

Outputs:
- A Markdown file `reports/brc/billion_row_challenge.md` by default, or a path
  provided via `--md-out`. The header includes the number of chunks and total
  bytes, to support plausibility/throughput analysis.
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
    """Timing and metadata for a single backend run.

    Attributes
    ----------
    backend: Name of the backend measured (e.g., ``"pandas"``, ``"dask"``).
    op: Operation key, ``"filter"`` or ``"groupby"``.
    read_s: Seconds spent reading and concatenating chunks.
    compute_s: Seconds spent executing the operation and materializing ``head``.
    rows: Number of rows observed in the materialized pandas output.
    used_cores: Approximate worker parallelism (if detectable) for the backend.
    version: Backend version string, if detectable.
    """
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
    """Parse CLI arguments and return a namespace.

    Returns
    -------
    argparse.Namespace
        The parsed arguments including rows-per-chunk, number of chunks,
        operation, optional existing data glob, optional specific backend,
        and optional Markdown output path.
    """
    p = argparse.ArgumentParser(description="Billion Row Challenge (safe scaffold)")
    args_list = [
        ("--rows-per-chunk", dict(type=int, default=1_000_000)),
        ("--num-chunks", dict(type=int, default=1)),
        ("--operation", dict(default="filter", choices=["filter", "groupby"])),
        ("--materialize", dict(default="head", choices=["head", "count", "all"])),
        ("--data-glob", dict(default=None)),
        ("--only-backend", dict(default=None)),
        ("--md-out", dict(default=None)),
    ]
    for name, kw in args_list:
        p.add_argument(name, **kw)
    return p.parse_args()


def resolve_chunks(args) -> List[Path]:
    """Resolve chunk paths from args.

    If ``--data-glob`` is provided, expand it to a list of existing files and
    fail if none found. Otherwise, generate size-specific CSV chunks on demand.
    """
    if args.data_glob:
        paths = existing_chunks(args.data_glob)
        if not paths:
            raise SystemExit(f"No files matched --data-glob '{args.data_glob}'")
        return paths
    return ensure_chunks(args.rows_per_chunk, args.num_chunks)


def read_frames_for_backend(chunks: List[Path], backend: str) -> List[Frame]:
    """Read chunk files into backend dataframes and wrap as ``Frame``.

    The IO path supports both Parquet and CSV based on each chunk's file
    extension.
    """
    frames: List[Frame] = []
    for p in chunks:
        if p.suffix.lower() == ".parquet":
            frames.append(read_parquet(str(p)))
        else:
            frames.append(read_csv(str(p)))
    return frames


def concat_frames(frames: List[Frame], backend: str) -> Frame:
    """Concatenate a list of Frames using backend-native concat semantics."""
    if backend == "pyspark":
        import pyspark.pandas as ps  # type: ignore
        return Frame(ps.concat([f.to_backend() for f in frames]))
    if backend == "dask":
        import dask.dataframe as dd  # type: ignore
        return Frame(dd.concat([f.to_backend() for f in frames]))
    import pandas as pd  # type: ignore
    return Frame(pd.concat([f.to_backend() for f in frames]))


def measure_read(chunks: List[Path], backend: str) -> tuple[Frame, float, int]:
    """Measure time to read and concatenate all chunks for a backend.

    Returns the combined Frame, elapsed seconds, and total input bytes.
    """
    t0 = time.perf_counter()
    frames = read_frames_for_backend(chunks, backend)
    combined = concat_frames(frames, backend)
    t1 = time.perf_counter()
    total_bytes = sum((p.stat().st_size for p in chunks if p.exists()), 0)
    return combined, t1 - t0, total_bytes


def _materialize_count(backend_df) -> int:
    """Return row count for different backends without collecting all rows.

    This forces full evaluation while avoiding a full to_pandas transfer.
    """
    try:
        import pandas as _pd  # type: ignore
        if isinstance(backend_df, _pd.DataFrame):
            return int(len(backend_df.index))
    except Exception:
        pass
    try:
        import dask.dataframe as _dd  # type: ignore
        from dask.dataframe import DataFrame as _DaskDF  # type: ignore
        if isinstance(backend_df, _DaskDF):
            return int(backend_df.shape[0].compute())
    except Exception:
        pass
    try:
        import pyspark.pandas as _ps  # type: ignore
        from pyspark.pandas.frame import DataFrame as _PsDF  # type: ignore
        if isinstance(backend_df, _PsDF):
            sdf = backend_df.to_spark()
            return int(sdf.count())
    except Exception:
        pass
    try:
        import polars as _pl  # type: ignore
        if isinstance(backend_df, _pl.DataFrame):
            return int(backend_df.height)
    except Exception:
        pass
    try:
        import duckdb as _duck  # type: ignore
        if hasattr(backend_df, "to_df"):
            # relation: COUNT(*) into pandas scalar
            con = _duck.connect()
            try:
                rel = backend_df
                return int(con.execute("SELECT COUNT(*) FROM rel").fetchone()[0])
            finally:
                con.close()
    except Exception:
        pass
    # Fallback
    try:
        return int(len(backend_df))
    except Exception:
        return 0


def run_operation(combined: Frame, op: str, materialize: str) -> tuple[int, float]:
    """Execute the chosen operation and materialize a small ``head``.

    Returns the observed row count in pandas and the compute duration.
    """
    t2 = time.perf_counter()
    out = combined.query("x > 0 and y < 0") if op == "filter" else combined.groupby("cat").agg({"x": "sum", "y": "mean"})
    if materialize == "head":
        pdf = out.head(10_000_000).to_pandas()
        rows = len(pdf.index) if hasattr(pdf, "index") else 0
    elif materialize == "count":
        # Force full evaluation by counting rows without transferring all data
        backend_obj = out.to_backend()
        rows = _materialize_count(backend_obj)
    else:  # materialize == "all"
        pdf_all = out.to_pandas()
        rows = len(pdf_all.index) if hasattr(pdf_all, "index") else 0
    t3 = time.perf_counter()
    return rows, t3 - t2


def run_backend(backend: str, chunks: List[Path], op: str) -> Result:
    """Run the full read+compute pipeline for a single backend and return timings."""
    configure_backend(backend)
    combined, read_s, _ = measure_read(chunks, backend)
    used = _used_cores_for_backend(backend)
    ver = get_backend_version(backend)
    # materialize mode from CLI via environment captured upstream; fall back to head
    import os as __os
    mat = __os.environ.get("BRC_MATERIALIZE", "head")
    rows, compute_s = run_operation(combined, op, mat)
    return Result(backend=backend, op=op, read_s=read_s, compute_s=compute_s, rows=rows, used_cores=used, version=ver)


def choose_backends(only_backend: Optional[str]) -> List[str]:
    """Return the list of backends to run, honoring ``--only-backend`` if set."""
    if only_backend:
        return [only_backend]
    return Backends


def build_rows(results: List[Result]) -> List[List[str]]:
    """Format results into string rows for fixed-width table rendering."""
    rows: List[List[str]] = []
    for r in results:
        rows.append([r.backend, str(r.version), r.op, f"{r.read_s:.4f}", f"{r.compute_s:.4f}", str(r.rows), str(r.used_cores)])
    return rows


def write_report(chunks: List[Path], results: List[Result], md_out: Optional[str]) -> None:
    """Write a fixed-width Markdown report including header context and timings."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    headers = ["backend", "version", "op", "read_s", "compute_s", "rows", "used_cores"]
    # All results share the same op/materialize for a given run
    op_val = results[0].op if results else "-"
    mat_val = os.environ.get("BRC_MATERIALIZE", "head")
    lines = [
        "# Billion Row Challenge (scaffold)",
        "",
        f"Generated at: {ts}",
        "",
        f"- operation: {op_val}",
        f"- materialize: {mat_val}",
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
    # Also export materialize for subprocess-based runners, while keeping CLI handling here
    os.environ["BRC_MATERIALIZE"] = args.materialize
    chunks = resolve_chunks(args)
    results: List[Result] = []
    backends_to_run = [args.only_backend] if args.only_backend else Backends
    for backend in backends_to_run:
        if not check_available(backend):
            continue
        results.append(run_backend(backend, chunks, args.operation))
    write_report(chunks, results, args.md_out)
    print(f"Ran BRC with operation={args.operation} materialize={args.materialize}")


if __name__ == "__main__":
    sys.exit(main())


