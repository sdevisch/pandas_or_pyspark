#!/usr/bin/env python3

from __future__ import annotations

"""Generate an API compatibility matrix across backends.

This script is an orchestrator: it builds the matrix and delegates all
markdown rendering to `src/mdreport`. No side effects at import time.
"""

import argparse
import sys
from typing import Callable, Dict, List, Tuple
from pathlib import Path

from unipandas import configure_backend
from unipandas.io import read_csv
from unipandas.frame import Frame

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
DATA = ROOT / "data"

# Use centralized list from scripts/utils.py to avoid drift
from scripts.utils import Backends  # type: ignore


def make_dataset() -> Path:
    """Ensure and return a tiny CSV for compatibility checks (idempotent)."""
    DATA.mkdir(exist_ok=True)
    p = DATA / "compat_small.csv"
    if p.exists():
        return p
    import pandas as pd  # type: ignore

    pdf = pd.DataFrame({"a": [1, 2, -1], "b": [10, -5, 0], "cat": ["x", "y", "x"]})
    pdf.to_csv(p, index=False)
    return p


def op_select(df: Frame) -> Frame:
    return df.select(["a", "b"])  # type: ignore


def op_query(df: Frame) -> Frame:
    return df.query("a > 0")


def op_assign(df: Frame) -> Frame:
    return df.assign(c=lambda x: x["a"] + x["b"])  # type: ignore


def op_groupby_agg(df: Frame) -> Frame:
    return df.groupby("cat").agg({"a": "sum"})


def op_merge(df: Frame) -> Frame:
    return df.merge(df, on="cat")


def op_sort_values(df: Frame) -> Frame:
    return df.sort_values("a")


def op_dropna(df: Frame) -> Frame:
    return df.dropna()


def op_fillna(df: Frame) -> Frame:
    return df.fillna(0)


def op_rename(df: Frame) -> Frame:
    return df.rename({"a": "A"})


def op_astype(df: Frame) -> Frame:
    return df.astype({"a": int})


OPS: List[Tuple[str, Callable[[Frame], Frame]]] = [
    ("select", op_select),
    ("query", op_query),
    ("assign", op_assign),
    ("groupby_agg", op_groupby_agg),
    ("merge", op_merge),
    ("sort_values", op_sort_values),
    ("dropna", op_dropna),
    ("fillna", op_fillna),
    ("rename", op_rename),
    ("astype", op_astype),
]


def try_ops_for_backend(backend: str, path: Path) -> Dict[str, str]:
    """Attempt all ops on a given backend, returning status per op name."""
    results: Dict[str, str] = {}
    # Optional availability short-circuit
    from scripts.utils import check_available as _check_available  # type: ignore

    if not _check_available(backend):
        for name, _ in OPS:
            results[name] = "unavailable"
        return results
    try:
        configure_backend(backend)
    except Exception:
        for name, _ in OPS:
            results[name] = "unavailable"
        return results

    try:
        df = read_csv(str(path))
    except Exception:
        for name, _ in OPS:
            results[name] = "load_fail"
        return results

    for name, fn in OPS:
        try:
            out = fn(df)
            _ = out.head(10).to_pandas()
            results[name] = "ok"
        except Exception:
            results[name] = "fail"
    return results


def _headers() -> List[str]:
    return ["operation", *Backends]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build an API compatibility matrix and write Markdown.")
    p.add_argument("--md-out", default=str(ROOT / "reports" / "api_demo" / "compatibility.md"))
    return p.parse_args()


def main() -> int:
    args = parse_args()
    path = make_dataset()

    hdr = _headers()
    rows: List[List[str]] = []

    backend_to_results: Dict[str, Dict[str, str]] = {}
    for backend in Backends:
        backend_to_results[backend] = try_ops_for_backend(backend, path)

    for op_name, _ in OPS:
        row = [op_name]
        for backend in Backends:
            row.append(backend_to_results[backend].get(op_name, "-"))
        rows.append(row)

    # Delegate Markdown rendering
    from mdreport.api_demo import render_compat_matrix  # type: ignore

    render_compat_matrix(args.md_out, hdr, rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
