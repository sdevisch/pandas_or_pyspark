#!/usr/bin/env python3

from __future__ import annotations

import sys
from typing import Callable, Dict, List, Tuple
from pathlib import Path
from datetime import datetime

from unipandas import configure_backend
from unipandas.io import read_csv
from unipandas.frame import Frame

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
REPORTS = ROOT / "reports" / "api_demo"
REPORTS.mkdir(exist_ok=True)
OUT = REPORTS / "compatibility.md"

Backends = ["pandas", "dask", "pyspark"]


def make_dataset() -> Path:
    DATA.mkdir(exist_ok=True)
    p = DATA / "compat_small.csv"
    if not p.exists():
        import pandas as pd

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
    results: Dict[str, str] = {}
    try:
        configure_backend(backend)
    except Exception as e:
        for name, _ in OPS:
            results[name] = f"unavailable ({e})"
        return results

    try:
        df = read_csv(str(path))
    except Exception as e:
        for name, _ in OPS:
            results[name] = f"load_fail ({e})"
        return results

    for name, fn in OPS:
        try:
            out = fn(df)
            _ = out.head(10).to_pandas()
            results[name] = "ok"
        except Exception as e:
            results[name] = f"fail ({type(e).__name__})"
    return results


def format_fixed_width(headers: List[str], rows: List[List[str]]) -> str:
    widths = [max(len(h), max((len(r[i]) for r in rows), default=0)) for i, h in enumerate(headers)]

    def fmt(vals: List[str]) -> str:
        parts: List[str] = []
        for i, v in enumerate(vals):
            align_left = i == 0
            parts.append(v.ljust(widths[i]) if align_left else v.rjust(widths[i]))
        return "  " + "  ".join(parts)

    lines: List[str] = [fmt(headers), "  " + "  ".join(["-" * w for w in widths])]
    for row in rows:
        lines.append(fmt(row))
    return "\n".join(lines)


def main():
    path = make_dataset()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    hdr = ["operation"] + Backends
    rows: List[List[str]] = []

    backend_to_results: Dict[str, Dict[str, str]] = {}
    for backend in Backends:
        backend_to_results[backend] = try_ops_for_backend(backend, path)

    for op_name, _ in OPS:
        row = [op_name]
        for backend in Backends:
            row.append(backend_to_results[backend].get(op_name, "-"))
        rows.append(row)

    content = ["# Compatibility matrix", "", f"Generated at: {ts}", "", "```text", format_fixed_width(hdr, rows), "```", ""]
    OUT.write_text("\n".join(content))
    print("Wrote", OUT)


if __name__ == "__main__":
    sys.exit(main())
