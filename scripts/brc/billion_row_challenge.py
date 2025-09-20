#!/usr/bin/env python3
"""Billion Row Challenge (BRC) scaffold.

This script provides a safe, repeatable harness for the Billion Row Challenge
across multiple data processing backends. It runs a single operation — a
groupby/aggregation — and logs the time to read/concatenate input chunks as
well as to compute the operation. Results are written as a fixed-width table
for alignment in Markdown.

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
Run on default 1B-row Parquet chunks (preferred):
    python scripts/brc/billion_row_challenge.py

Run a single backend explicitly:
    UNIPANDAS_BACKEND=pyspark python scripts/brc/billion_row_challenge.py --only-backend pyspark

Force full compute (instead of head) and write report to custom path:
    python scripts/brc/billion_row_challenge.py --materialize count --md-out reports/brc/custom.md

CLI Flags
---------
- --materialize: "head" (default), "count" (full compute w/o full transfer), or "all"
- --data-glob: Glob pattern for existing inputs (Parquet only). Defaults to 1B rows.
- --only-backend: Restrict run to a single backend
- --md-out: Output markdown path

Environment
-----------
- UNIPANDAS_BACKEND: Preferred backend (pandas, dask, pyspark, polars, duckdb)
- BRC_MATERIALIZE: Materialization override (head|count|all). Set automatically by --materialize.

Notes
-----
- BRC is Parquet-only. CSV is not supported.
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
    from .brc_core import compute_op_and_count  # type: ignore
except Exception:
    # Allow running as a standalone script
    from brc_shared import read_frames_for_backend, concat_frames, measure_read, run_operation  # type: ignore
    from brc_core import compute_op_and_count  # type: ignore
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


# Try to import mdreport for standardized Markdown writing
try:  # First attempt: assume package available on sys.path
    from mdreport import Report as _MdReport  # type: ignore
except Exception:
    try:
        _src = ROOT / "src"
        if str(_src) not in sys.path:
            sys.path.insert(0, str(_src))
        from mdreport import Report as _MdReport  # type: ignore
    except Exception:  # Fallback shim: define a tiny wrapper using utils formatter
        _MdReport = None  # type: ignore


def _chunks_out_dir(rows_per_chunk: int) -> Path:
    DATA.mkdir(exist_ok=True)
    out = DATA / f"brc_{rows_per_chunk}"
    out.mkdir(exist_ok=True)
    return out


def _write_rows_csv(path: Path, rows: int, base: int, rnd) -> None:
    return


def _maybe_generate_chunk(out_dir: Path, rows_per_chunk: int, i: int, rnd) -> Path:
    return out_dir / f"brc_{rows_per_chunk}_{i:04d}.csv"


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
    groups: Optional[int] = None


def get_backend_version(backend: str) -> Optional[str]:
    return utils_get_backend_version(backend)


def _used_cores_for_backend(backend: str) -> Optional[int]:
    return utils_used_cores_for_backend(backend)


def check_available(backend: str) -> bool:
    return utils_check_available(backend)


def format_fixed(headers: List[str], rows: List[List[str]]) -> List[str]:
    return utils_format_fixed(headers, rows)


def parse_arguments():
    """Parse CLI arguments and return a namespace (groupby-only).

    Returns
    -------
    argparse.Namespace
        The parsed arguments including optional existing data glob, optional
        specific backend, materialization mode, and optional Markdown output
        path. Defaults to 1B-row Parquet glob.
    """
    p = argparse.ArgumentParser(description="Billion Row Challenge (groupby-only)")
    p.add_argument("--materialize", default="count", choices=["head", "count", "all"])
    p.add_argument("--data-glob", default=None)
    p.add_argument("--only-backend", default=None)
    p.add_argument("--md-out", default=None)
    p.add_argument("--jsonl-out", default=None)
    p.add_argument("--no-md", action="store_true")
    return p.parse_args()


def resolve_chunks(args) -> List[Path]:
    """Resolve chunk paths from args (Parquet-only; default to 1B rows glob).

    If ``--data-glob`` is provided, expand it to a list of existing files and
    fail if none found. Otherwise, default to the 1B-row Parquet scale glob.
    """
    if args.data_glob:
        paths = existing_chunks(args.data_glob)
        if not paths:
            raise SystemExit(f"No files matched --data-glob '{args.data_glob}'")
        return paths
    # Default to pre-generated 1B parquet scale
    root = Path(__file__).resolve().parents[2]
    default_glob = str(root / "data/brc_scales/parquet_1000000000/*.parquet")
    paths = existing_chunks(default_glob)
    if not paths:
        raise SystemExit("Default 1B Parquet data not found. Provide --data-glob.")
    return paths




def _compute_input_rows_general(chunks: List[Path], args) -> Optional[int]:
    """Total input rows across chunks (Parquet-only)."""
    return _total_rows_from_parquet(chunks)


def _detect_source(chunks: List[Path]) -> str:
    """Parquet-only source label for reports."""
    return "parquet"


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
    out = combined.groupby("cat").agg({"x": "sum", "y": "mean"})
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
        # Enforce consistent category order for readability
        try:
            if "cat" in out.columns:
                import pandas as _pd  # type: ignore

                ordered = _pd.Categorical(out["cat"], categories=["x", "y", "z"], ordered=True)
                out = out.assign(cat=ordered).sort_values("cat").reset_index(drop=True)
        except Exception:
            try:
                out = out.sort_values(list(out.columns)[0]).reset_index(drop=True)
            except Exception:
                pass
        out = out.head(limit)
        headers = ["backend"] + [str(c) for c in list(out.columns)]
        rows = [[backend] + [str(v) for v in row] for row in out.to_records(index=False)]
        return format_fixed(headers, rows)
    except Exception:
        return ["(preview unavailable)"]


def run_backend(backend: str, chunks: List[Path], op: str, input_rows: Optional[int]) -> Result:
    """Run the full read+compute pipeline for a single backend and return timings."""
    configure_backend(backend)
    combined, read_s, _ = measure_read(chunks, backend)
    used = _used_cores_for_backend(backend)
    ver = get_backend_version(backend)
    # materialize mode from CLI via environment captured upstream; fall back to head
    import os as __os
    mat = __os.environ.get("BRC_MATERIALIZE", "head")
    try:
        rows, compute_s = compute_op_and_count(combined, "groupby", mat)
    except Exception:
        rows, compute_s = run_operation(combined, "groupby", mat)
    if True:
        # For groupby, interpret the counted rows as number of output groups
        return Result(
            backend=backend,
            op=op,
            read_s=read_s,
            compute_s=compute_s,
            rows=input_rows,  # report input rows processed
            used_cores=used,
            version=ver,
            groups=rows,
        )
    return Result(
        backend=backend,
        op="groupby",
        read_s=read_s,
        compute_s=compute_s,
        rows=rows,
        used_cores=used,
        version=ver,
        groups=None,
    )


def choose_backends(only_backend: Optional[str]) -> List[str]:
    """Return the list of backends to run, honoring ``--only-backend`` if set."""
    if only_backend:
        return [only_backend]
    return Backends


def build_rows(results: List[Result], *, include_groups: bool = False) -> List[List[str]]:
    """Format results into string rows for fixed-width table rendering."""
    rows: List[List[str]] = []
    for r in results:
        def fmt_num(v: Optional[float]) -> str:
            return f"{v:.4f}" if isinstance(v, float) and v > 0 else "-"
        base = [
            r.backend,
            str(r.version),
            r.op,
            fmt_num(r.read_s if r.read_s is not None else None),
            fmt_num(r.compute_s if r.compute_s is not None else None),
            str(r.rows) if r.rows is not None else "-",
            str(r.used_cores) if r.used_cores is not None else "-",
        ]
        if include_groups:
            # Show number of output groups in a dedicated column
            base.append(str(r.groups) if r.groups is not None else "-")
        rows.append(base)
    return rows


def _system_info_lines() -> List[str]:
    import platform as _platform
    import os as _osmod
    lines: List[str] = []
    lines.append(f"- Python: `{_platform.python_version()}` on `{_platform.platform()}`")
    lines.append(f"- CPU cores: {_osmod.cpu_count()}")
    # Try to add total memory if psutil is available
    try:
        import psutil as _ps  # type: ignore

        mem_gb = getattr(_ps.virtual_memory(), "total", 0) / (1024 ** 3)
        lines.append(f"- RAM: {mem_gb:.1f} GiB")
    except Exception:
        pass
    return lines


def write_report(chunks: List[Path], results: List[Result], md_out: Optional[str], *, append: bool = False, title_suffix: str = "", input_rows_override: Optional[int] = None) -> None:
    """Write a fixed-width Markdown report via mdreport if available (fallback to utils)."""
    headers = ["backend", "version", "op", "read_s", "compute_s", "rows", "used_cores"]
    op_val = "groupby"
    include_groups = any(getattr(r, "groups", None) is not None for r in results)
    if include_groups:
        headers = headers + ["groups"]
    mat_val = os.environ.get("BRC_MATERIALIZE", "head")
    source = _detect_source(chunks)
    input_rows = input_rows_override if input_rows_override is not None else _total_rows_from_parquet(chunks)
    # Protect main report: if input rows are less than 1B, route to smoke report unless explicitly overridden
    default_out = OUT
    smoke_out = REPORTS / "billion_row_challenge_smoke.md"
    effective_out = None
    if md_out:
        effective_out = Path(md_out)
    else:
        try:
            inp = int(input_rows) if input_rows is not None else None
        except Exception:
            inp = None
        if inp is not None and inp < 1_000_000_000:
            effective_out = smoke_out
        else:
            effective_out = default_out
    out_path = effective_out
    preface = [
        f"- operation: {op_val}",
        f"- materialize: {mat_val}",
        f"- num_chunks: {len(chunks)}",
        f"- total_bytes: {sum((p.stat().st_size for p in chunks if p.exists()), 0)}",
        f"- source: {source}",
        f"- input_rows: {input_rows if input_rows is not None else '-'}",
        *( ["- note: routed to smoke report (< 1B input rows)"] if out_path.name == "billion_row_challenge_smoke.md" else [] ),
        *(_system_info_lines()),
        "",
    ]
    rows = build_rows(results, include_groups=include_groups)
    suffix = None
    if results:
        suffix_lines = ["Groupby result preview (by backend):", "", "```text"]
        for r in results:
            if r.backend:
                suffix_lines.extend(_build_groupby_preview_lines(chunks, r.backend))
        suffix_lines.extend(["```", ""])
        suffix = suffix_lines
    
    if _MdReport is not None:
        rpt = _MdReport(out_path)
        rpt.title("Billion Row Challenge (scaffold)" + (f" - {title_suffix}" if title_suffix else ""))
        rpt.preface(preface)
        rpt.table(headers, rows, align_from=3, style="fixed")
        if suffix:
            rpt.suffix(suffix)
        rpt.write(append=append)
    else:
        # Fallback to existing utils writer if mdreport is not importable
        if append and out_path.exists():
            existing = out_path.read_text()
            out_path.write_text(existing + "\n\n")
        try:
            from scripts.utils import write_fixed_markdown as _write  # type: ignore
        except Exception:
            from utils import write_fixed_markdown as _write  # type: ignore
        _write(
            out_path=out_path,
            title="Billion Row Challenge (scaffold)" + (f" - {title_suffix}" if title_suffix else ""),
            headers=headers,
            rows=rows,
            preface_lines=preface,
            right_align_from=3,
            suffix_lines=suffix,
        )

def main():
    args = parse_arguments()
    # Also export materialize for subprocess-based runners, while keeping CLI handling here
    os.environ["BRC_MATERIALIZE"] = args.materialize
    chunks = resolve_chunks(args)
    results: List[Result] = []
    # Build rows for all known backends, including unavailable ones, so the
    # report always shows a complete matrix of backends with their status.
    availability = {b: check_available(b) for b in Backends}
    def run_for_op(*, append: bool, title: str, include_placeholders: bool) -> None:
        results_local: List[Result] = []
        input_rows_total = _compute_input_rows_general(chunks, args)
        for backend in backends_to_run:
            if not availability.get(backend, False):
                continue
            results_local.append(run_backend(backend, chunks, "groupby", input_rows_total))
        if include_placeholders:
            have = {r.backend for r in results_local}
            for backend in Backends:
                if backend not in have:
                    results_local.append(
                        Result(
                            backend=backend,
                            op="groupby",
                            read_s=None,  # type: ignore
                            compute_s=None,  # type: ignore
                            rows=None,
                            used_cores=None,
                            version=get_backend_version(backend),
                        )
                    )
        # Write JSONL if requested
        if args.jsonl_out:
            try:
                from perfcore.result import Result as _PerfResult  # type: ignore
                from perfcore.measure import write_results as _write_jsonl  # type: ignore
                # Map local Result to PerfResult
                perfs = []
                for r in results_local:
                    pr = _PerfResult.now(frontend="unipandas", backend=r.backend, operation="groupby")
                    pr.input_rows = input_rows_total
                    pr.read_seconds = float(r.read_s) if r.read_s is not None else None
                    pr.compute_seconds = float(r.compute_s) if r.compute_s is not None else None
                    pr.groups = r.groups
                    pr.used_cores = r.used_cores
                    pr.ok = r.compute_s is not None and r.read_s is not None
                    perfs.append(pr)
                from pathlib import Path as _Path
                _write_jsonl(perfs, _Path(args.jsonl_out))
            except Exception:
                pass
        # Optionally write Markdown
        if not args.no_md:
            write_report(chunks, results_local, args.md_out, append=append, title_suffix=title, input_rows_override=input_rows_total)

    backends_to_run = [args.only_backend] if args.only_backend else Backends
    # Always include placeholders so the report shows a full matrix even when a single backend is run
    include_ph = True
    run_for_op(append=False, title="groupby", include_placeholders=include_ph)
    print(f"Ran BRC (groupby) with materialize={args.materialize}")


if __name__ == "__main__":
    sys.exit(main())


