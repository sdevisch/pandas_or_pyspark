#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
REPORTS.mkdir(exist_ok=True)
OUT = REPORTS / "brc_scale.md"

PY = sys.executable or "python3"
SCRIPT = ROOT / "scripts" / "brc" / "billion_row_challenge.py"

Backends = ["pandas", "dask", "pyspark", "polars", "duckdb"]
SCALES = [1_000_000, 10_000_000, 100_000_000, 1_000_000_000]


def fmt_fixed(headers: List[str], rows: List[List[str]]) -> List[str]:
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


def run_step(backend: str, rows: int, budget_s: float, op: str) -> bool:
    env = os.environ.copy()
    env["UNIPANDAS_BACKEND"] = backend
    cmd = [
        PY,
        str(SCRIPT),
        "--rows-per-chunk",
        str(rows),
        "--num-chunks",
        "1",
        "--operation",
        op,
    ]
    try:
        subprocess.run(cmd, env=env, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, timeout=budget_s)
        return True
    except subprocess.TimeoutExpired:
        return False
    except Exception:
        return False


def main():
    parser = argparse.ArgumentParser(description="BRC scale runner (1M→1B) with 3-minute per-step cap")
    parser.add_argument("--budget", type=float, default=180.0, help="Seconds per backend-size step")
    parser.add_argument("--operation", default="filter", choices=["filter", "groupby"], help="Operation to run")
    args = parser.parse_args()

    headers = ["backend", "rows", "ok"]
    rows_out: List[List[str]] = []

    for backend in Backends:
        proceed = True
        for size in SCALES:
            if not proceed:
                break
            ok = run_step(backend, size, args.budget, args.operation)
            rows_out.append([backend, f"{size}", "yes" if ok else "no"])
            proceed = ok

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    content = ["# BRC scale runner", "", f"Generated at: {ts}", "", "```text", *fmt_fixed(headers, rows_out), "```", ""]
    OUT.write_text("\n".join(content))
    print("Wrote", OUT)


if __name__ == "__main__":
    sys.exit(main())


