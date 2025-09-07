#!/usr/bin/env python3
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

Usage
-----
Basic run (generate small CSV chunks and filter):
    python scripts/brc/billion_row_challenge.py --rows-per-chunk 100000 --num-chunks 2 --operation filter

Run on existing Parquet chunks:
    python scripts/brc/billion_row_challenge.py --data-glob "data/brc_100000/*.parquet" --operation groupby

Run a single backend explicitly:
    UNIPANDAS_BACKEND=pyspark python scripts/brc/billion_row_challenge.py --only-backend pyspark

Force full compute (instead of head) and write report to custom path:
    python scripts/brc/billion_row_challenge.py --materialize count --md-out reports/brc/custom.md

CLI Flags
---------
- --rows-per-chunk: Integer row count per generated CSV chunk (default 1_000_000)
- --num-chunks: Number of chunks to generate (default 1)
- --operation: "filter" or "groupby" (default "filter")
- --materialize: "head" (default), "count" (full compute w/o full transfer), or "all"
- --data-glob: Glob pattern for existing inputs (Parquet or CSV)
- --only-backend: Restrict run to a single backend
- --md-out: Output markdown path

Environment
-----------
- UNIPANDAS_BACKEND: Preferred backend (pandas, dask, pyspark, polars, duckdb)
- BRC_MATERIALIZE: Materialization override (head|count|all). Set automatically by --materialize.

Notes
-----
- CSV generation uses size-specific directories to avoid reusing small files for larger sizes.
- For lazy engines (Dask, pandas-on-Spark), "count" is recommended to force full compute without
  a full to_pandas transfer.
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
from unipandas.frame import Frame
try:
    from .brc_shared import read_frames_for_backend, concat_frames, measure_read, run_operation  # type: ignore
except Exception:
    # Allow running as a standalone script
    from brc_shared import read_frames_for_backend, concat_frames, measure_read, run_operation  # type: ignore
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
        ("--operation", dict(default="groupby", choices=["filter", "groupby"])),
        ("--materialize", dict(default="count", choices=["head", "count", "all"])),
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




def _detect_source(chunks: List[Path]) -> str:
    """Return 'parquet' if any chunk is .parquet, else 'csv'."""
    for p in chunks:
        if p.suffix.lower() == ".parquet":
            return "parquet"
    return "csv"


def _total_rows_from_parquet(chunks: List[Path]) -> Optional[int]:
    """Sum row counts from Parquet metadata footers (cheap and exact)."""
    try:
        import pyarrow.parquet as _pq  # type: ignore
    except Exception:
        return None
    total = 0
    any_parquet = False
    for p in chunks:
        if p.suffix.lower() == ".parquet" and p.exists():
            any_parquet = True
            try:
                total += int(_pq.ParquetFile(str(p)).metadata.num_rows)
            except Exception:
                return None
    return total if any_parquet else None




 


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
            con = _duck.connect()
            try:
                rel = backend_df
                return int(con.execute("SELECT COUNT(*) FROM rel").fetchone()[0])
            finally:
                con.close()
    except Exception:
        pass
    try:
        return int(len(backend_df))
    except Exception:
        return 0


def run_operation(combined: Frame, op: str, materialize: str) -> tuple[int, float]:
    """Execute the chosen operation and fully evaluate the result.

    We intentionally avoid partial materialization (e.g., ``head``) to ensure
    the full dataset flows through the pipeline for true billion-row runs.

    Returns the observed row count (backend-native) and the compute duration.
    """
    t2 = time.perf_counter()
    out = combined.query("x > 0 and y < 0") if op == "filter" else combined.groupby("cat").agg({"x": "sum", "y": "mean"})
    # Normalize any "head" request to a full count to avoid size reduction
    if materialize == "head":
        materialize = "count"
    if materialize == "count":
        # Force full evaluation by counting rows without transferring all data
        backend_obj = out.to_backend()
        rows = _materialize_count(backend_obj)
    else:  # materialize == "all"
        pdf_all = out.to_pandas()
        rows = len(pdf_all.index) if hasattr(pdf_all, "index") else 0
    t3 = time.perf_counter()
    return rows, t3 - t2


def _build_groupby_preview_lines(chunks: List[Path], backend: str, limit: int = 10) -> List[str]:
    """Create fixed-width lines showing the first groupby rows for context.

    This is meant for report readability and does not aim to be exhaustive.
    """
    try:
        configure_backend(backend)
        frames = read_frames_for_backend(chunks, backend)
        combined = concat_frames(frames, backend)
        out = combined.groupby("cat").agg({"x": "sum", "y": "mean"}).to_pandas()
        try:
            out = out.reset_index()  # ensure 'cat' is a column for printing
        except Exception:
            pass
        out = out.head(limit)
        headers = [str(c) for c in list(out.columns)]
        rows = [[str(v) for v in row] for row in out.to_records(index=False)]
        return format_fixed(headers, rows)
    except Exception:
        return ["(preview unavailable)"]


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
    """Write a fixed-width Markdown report including header context and timings.

    Show all result lines verbatim without truncation.
    """
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    headers = ["backend", "version", "op", "read_s", "compute_s", "rows", "used_cores"]
    op_val = results[0].op if results else "-"
    mat_val = os.environ.get("BRC_MATERIALIZE", "head")
    source = _detect_source(chunks)
    input_rows = _total_rows_from_parquet(chunks)
    rows_text = format_fixed(headers, build_rows(results))
    lines = [
        "# Billion Row Challenge (scaffold)",
        "",
        f"Generated at: {ts}",
        "",
        f"- operation: {op_val}",
        f"- materialize: {mat_val}",
        f"- num_chunks: {len(chunks)}",
        f"- total_bytes: {sum((p.stat().st_size for p in chunks if p.exists()), 0)}",
        f"- source: {source}",
        f"- input_rows: {input_rows if input_rows is not None else '-'}",
        "",
        "```text",
        *rows_text,
        "```",
        "",
    ]
    # Add a small preview of the groupby output to demonstrate actual results
    if op_val == "groupby":
        lines.extend([
            "Groupby result preview:",
            "",
            "```text",
            *_build_groupby_preview_lines(chunks, results[0].backend if results else "pandas"),
            "```",
            "",
        ])
    out_path = Path(md_out) if md_out else OUT
    out_path.write_text("\n".join(lines))
    print("Wrote", out_path)

def main():
    args = parse_arguments()
    # Also export materialize for subprocess-based runners, while keeping CLI handling here
    os.environ["BRC_MATERIALIZE"] = args.materialize
    chunks = resolve_chunks(args)
    results: List[Result] = []
    # Build rows for all known backends, including unavailable ones, so the
    # report always shows a complete matrix of backends with their status.
    availability = {b: check_available(b) for b in Backends}
    backends_to_run = [args.only_backend] if args.only_backend else Backends
    for backend in backends_to_run:
        if not availability.get(backend, False):
            continue
        results.append(run_backend(backend, chunks, args.operation))
    # Insert placeholders for unavailable backends to make them visible.
    have = {r.backend for r in results}
    for backend in Backends:
        if backend not in have:
            results.append(
                Result(
                    backend=backend,
                    op=args.operation,
                    read_s=0.0,
                    compute_s=0.0,
                    rows=None,
                    used_cores=None,
                    version=get_backend_version(backend),
                )
            )
    write_report(chunks, results, args.md_out)
    print(f"Ran BRC with operation={args.operation} materialize={args.materialize}")


if __name__ == "__main__":
    sys.exit(main())


