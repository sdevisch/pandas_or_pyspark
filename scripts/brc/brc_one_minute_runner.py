#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime
import glob
from pathlib import Path
from typing import List
import time

ROOT = Path(__file__).resolve().parents[2]
REPORTS = ROOT / "reports" / "brc"
REPORTS.mkdir(parents=True, exist_ok=True)
OUT = REPORTS / "brc_one_minute.md"

PY = sys.executable or "python3"
SCRIPT = ROOT / "scripts" / "brc" / "billion_row_challenge.py"

Backends = ["pandas", "dask", "pyspark", "polars", "duckdb"]

SCALES = [1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000]


def fmt_fixed(headers: List[str], rows: List[List[str]]) -> List[str]:
    widths = [max(len(headers[i]), max((len(r[i]) for r in rows), default=0)) for i in range(len(headers))]

    def fmt_row(vals: List[str]) -> str:
        parts: List[str] = []
        for i, v in enumerate(vals):
            if i < 1:
                parts.append(v.ljust(widths[i]))
            else:
                parts.append(v.rjust(widths[i]))
        return "  ".join(parts)

    lines = [fmt_row(headers), "  ".join(("-" * w) for w in widths)]
    for r in rows:
        lines.append(fmt_row(r))
    return lines


def sci(n: int) -> str:
    return f"{n:.1e}"


def can_process_within(backend: str, rows: int, budget_s: float, operation: str, data_glob: str | None) -> tuple[bool, float]:
    env = os.environ.copy()
    env["UNIPANDAS_BACKEND"] = backend
    cmd = [PY, str(SCRIPT), "--rows-per-chunk", str(rows), "--num-chunks", "1", "--operation", operation]
    if data_glob:
        cmd = [PY, str(SCRIPT), "--data-glob", data_glob, "--operation", operation]
    start = time.perf_counter()
    try:
        subprocess.run(cmd, env=env, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, timeout=budget_s)
        elapsed = time.perf_counter() - start
        return True, elapsed
    except Exception:
        elapsed = time.perf_counter() - start
        return False, elapsed


def main():
    parser = argparse.ArgumentParser(description="1-minute BRC runner: max rows per backend under 60s")
    parser.add_argument("--budget", type=float, default=60.0, help="Seconds per attempt")
    parser.add_argument("--operation", choices=["filter", "groupby"], default="filter")
    parser.add_argument("--data-glob", default=None, help="Optional glob to use existing data instead of generating")
    parser.add_argument(
        "--data-glob-template",
        default=None,
        help="Optional template with {size} placeholder, e.g. 'data/brc_scales/parquet_{size}/*.parquet'",
    )
    args = parser.parse_args()

    headers = ["backend", "max_rows_within_1min", "elapsed_s_at_max"]
    rows_out: List[List[str]] = []

    for backend in Backends:
        max_rows = 0
        elapsed_at_max = 0.0
        for size in SCALES:
            glob_arg = args.data_glob
            if args.data_glob_template:
                glob_arg = args.data_glob_template.format(size=size)
                # If no files exist for this size, skip without breaking
                if not glob.glob(glob_arg):
                    continue
            ok, elapsed = can_process_within(backend, size, args.budget, args.operation, glob_arg)
            if ok:
                max_rows = size
                elapsed_at_max = elapsed
            else:
                break
        rows_out.append([backend, sci(max_rows), f"{elapsed_at_max:.3f}"])

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    content = ["# 1-minute BRC runner", "", f"Generated at: {ts}", "", "```text", *fmt_fixed(headers, rows_out), "```", ""]
    OUT.write_text("\n".join(content))
    print("Wrote", OUT)


if __name__ == "__main__":
    sys.exit(main())


