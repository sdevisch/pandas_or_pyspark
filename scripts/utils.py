from __future__ import annotations

import os
from typing import Dict, List, Optional
from pathlib import Path
from datetime import datetime


Backends = ["pandas", "dask", "pyspark", "polars", "duckdb", "numpy", "numba"]


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
        if backend == "polars":
            import polars as pl  # type: ignore

            return getattr(pl, "__version__", None)
        if backend == "duckdb":
            import duckdb  # type: ignore

            return getattr(duckdb, "__version__", None)
        if backend == "numpy":
            import numpy as np  # type: ignore

            return getattr(np, "__version__", None)
        if backend == "numba":
            import numba  # type: ignore

            return getattr(numba, "__version__", None)
    except Exception:
        return None
    return None


def used_cores_for_backend(backend: str) -> Optional[int]:
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
        if backend == "polars":
            # Polars executes single-process by default
            return 1
        if backend == "duckdb":
            # DuckDB runs in-process
            return 1
        if backend == "numpy":
            return 1
        if backend == "numba":
            return 1
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
        elif backend == "polars":
            __import__("polars")
        elif backend == "duckdb":
            __import__("duckdb")
        elif backend == "numpy":
            __import__("numpy")
        elif backend == "numba":
            __import__("numba")
        else:
            return False
        return True
    except Exception:
        return False


def format_fixed(headers: List[str], rows: List[List[str]], right_align_from: int = 2) -> List[str]:
    widths = [max(len(headers[i]), max((len(r[i]) for r in rows), default=0)) for i in range(len(headers))]

    def fmt_row(vals: List[str]) -> str:
        parts: List[str] = []
        for i, v in enumerate(vals):
            if i < right_align_from:
                parts.append(v.ljust(widths[i]))
            else:
                parts.append(v.rjust(widths[i]))
        return "  ".join(parts)

    lines = [fmt_row(headers), "  ".join(("-" * w) for w in widths)]
    for r in rows:
        lines.append(fmt_row(r))
    return lines


def write_fixed_markdown(
    out_path: Path,
    title: str,
    headers: List[str],
    rows: List[List[str]],
    preface_lines: Optional[List[str]] = None,
    right_align_from: int = 2,
    suffix_lines: Optional[List[str]] = None,
) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines: List[str] = [f"# {title}", "", f"Generated at: {ts}", ""]
    if preface_lines:
        lines.extend(preface_lines)
    lines.extend(["```text", *format_fixed(headers, rows, right_align_from=right_align_from), "```", ""])
    if suffix_lines:
        lines.extend(suffix_lines)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines))
    print("Wrote", out_path)


# -----------------------
# BRC OM-specific helpers
# -----------------------

def om_headers() -> List[str]:
    """Default headers for the Billion Row OM runner report."""
    return [
        "backend",
        "rows(sci)",
        "operation",
        "source",
        "read_s",
        "compute_s",
        "input_rows",
        "ok",
        "sanity_ok",
    ]


def write_brc_om_report(out_path: Path, rows: List[List[str]]) -> None:
    """Write the Billion Row OM runner report using standard formatting."""
    write_fixed_markdown(
        out_path=out_path,
        title="Billion Row OM Runner",
        headers=om_headers(),
        rows=rows,
        preface_lines=None,
        right_align_from=4,
    )
