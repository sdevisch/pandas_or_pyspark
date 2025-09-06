#!/usr/bin/env python3

from __future__ import annotations

import argparse
import concurrent.futures
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
REPORTS.mkdir(exist_ok=True)
OUT = REPORTS / "billion_row_om.md"

PY = sys.executable or "python3"
SCRIPT = ROOT / "scripts" / "billion_row_challenge.py"

Backends = ["pandas", "dask", "pyspark"]
ORDERS = [1_000, 10_000, 100_000, 1_000_000]
DEFAULT_BUDGET_S = 180.0


@dataclass
class Entry:
    backend: str
    rows: int
    read_s: Optional[float]
    compute_s: Optional[float]
    ok: bool


def run_once(backend: str, rows: int, budget_s: float) -> Entry:
    # Call the underlying script with rows_per_chunk=rows and num_chunks=1, operation=filter
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
        "filter",
    ]
    start = time.time()
    try:
        # Capture stdout to parse timings from the generated report
        subprocess.run(
            cmd,
            env=env,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=budget_s,
        )
        # Parse the latest report file to extract last line timings for this backend
        report_path = REPORTS / "billion_row_challenge.md"
        read_s = None
        compute_s = None
        if report_path.exists():
            lines = report_path.read_text().strip().splitlines()
            # find line that starts with backend
            for line in lines[::-1]:
                if line.startswith(backend):
                    parts = line.split()
                    # backend version op read_s compute_s rows used_cores
                    # pick read_s and compute_s
                    if len(parts) >= 6:
                        try:
                            read_s = float(parts[3])
                            compute_s = float(parts[4])
                        except Exception:
                            pass
                    break
        return Entry(backend=backend, rows=rows, read_s=read_s, compute_s=compute_s, ok=True)
    except subprocess.TimeoutExpired:
        return Entry(backend=backend, rows=rows, read_s=None, compute_s=None, ok=False)
    except Exception:
        return Entry(backend=backend, rows=rows, read_s=None, compute_s=None, ok=False)


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


def main():
    parser = argparse.ArgumentParser(description="Order-of-magnitude BRC runner with per-step 3-minute cap")
    parser.add_argument("--budgets", type=float, default=DEFAULT_BUDGET_S, help="Seconds per backend-size step")
    args = parser.parse_args()

    budget = float(args.budgets)

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows: List[List[str]] = []
    headers = ["backend", "rows", "read_s", "compute_s", "ok"]

    for backend in Backends:
        proceed = True
        for size in ORDERS:
            if not proceed:
                break
            entry = run_once(backend, size, budget)
            rows.append([
                backend,
                f"{size}",
                f"{entry.read_s:.4f}" if entry.read_s is not None else "-",
                f"{entry.compute_s:.4f}" if entry.compute_s is not None else "-",
                "yes" if entry.ok else "no",
            ])
            proceed = entry.ok

    content = [
        "# Billion Row OM Runner",
        "",
        f"Generated at: {ts}",
        "",
        "```text",
        *fmt_fixed(headers, rows),
        "```",
        "",
    ]
    OUT.write_text("\n".join(content))
    print("Wrote", OUT)


if __name__ == "__main__":
    sys.exit(main())
