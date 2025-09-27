#!/usr/bin/env python3

from __future__ import annotations

"""01 - One-minute groupby capacity test across backends.

For each backend, tries escalating row sizes and records the largest size
that completes a groupby aggregation within the per-step budget.
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts.brc.brc_paths import REPORTS_BRC as REPORTS  # type: ignore
OUT = REPORTS / "brc_under_1min_capacity.md"

PY = sys.executable or "python3"
SCRIPT = ROOT / "scripts" / "brc" / "billion_row_challenge.py"

from scripts.utils import Backends as Backends  # type: ignore
from scripts.brc.one_minute_lib import can_process_within  # type: ignore

# Sizes to attempt (rows). Stop escalating once a step fails for a backend.
SCALES = [1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000]


 


def main() -> int:
    parser = argparse.ArgumentParser(description="One-minute groupby capacity test across backends")
    parser.add_argument("--budget", type=float, default=60.0, help="Seconds per attempt")
    parser.add_argument("--data-glob-template", default=None, help="Optional '{size}' template to force an existing parquet glob")
    args = parser.parse_args()

    headers = ["backend", "max_rows_within_1min", "elapsed_s_at_max"]
    rows_out: List[List[str]] = []

    for backend in Backends:
        max_rows = 0
        elapsed_at_max = 0.0
        for size in SCALES:
            glob_arg = args.data_glob_template.format(size=size) if args.data_glob_template else None
            ok, elapsed = can_process_within(backend, size, args.budget, glob_arg)
            if ok:
                max_rows = size
                elapsed_at_max = elapsed
            else:
                break
        rows_out.append([backend, f"{max_rows:.1e}", f"{elapsed_at_max:.3f}"])

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        from mdreport import Report  # type: ignore
    except Exception:
        # Fallback: write a minimal fixed-width table
        widths = [max(len(headers[i]), max((len(r[i]) for r in rows_out), default=0)) for i in range(len(headers))]
        def fmt_row(vals: List[str]) -> str:
            return "  ".join(v.ljust(widths[i]) if i < 1 else v.rjust(widths[i]) for i, v in enumerate(vals))
        lines = ["# 1-minute BRC runner", "", f"Generated at: {ts}", "", "```text", fmt_row(headers), "  ".join("-" * w for w in widths)]
        for r in rows_out:
            lines.append(fmt_row(r))
        lines += ["```", ""]
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text("\n".join(lines))
        return 0

    rpt = Report(OUT)
    rpt.title("1-minute BRC runner").preface([f"Generated at: {ts}", ""]) \
       .table(headers, rows_out, align_from=1, style="fixed").write()
    return 0


if __name__ == "__main__":
    sys.exit(main())
