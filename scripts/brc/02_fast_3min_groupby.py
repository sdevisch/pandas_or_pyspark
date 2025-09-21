#!/usr/bin/env python3

from __future__ import annotations

"""02 - Fastest backends: 3-minute groupby capacity per backend.

- Phase 1: Measure baseline throughput for each backend on a medium size.
- Phase 2: Keep top-N backends and find the largest size they can complete
  within the budget (default 180s) for a groupby aggregation.
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.brc.brc_paths import REPORTS_BRC as REPORTS  # type: ignore
OUT = REPORTS / "brc_fast_3min_groupby.md"

PY = sys.executable or "python3"
SCRIPT = ROOT / "scripts" / "brc" / "billion_row_challenge.py"
from scripts.utils import Backends as Backends  # type: ignore

SCALES = [
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
]


def _preferred_glob(size: int) -> Optional[str]:
    scales_dir = ROOT / "data" / "brc_scales" / f"parquet_{size}"
    if scales_dir.exists() and any(scales_dir.glob("*.parquet")):
        return str(scales_dir / "*.parquet")
    return None


def _run_once(backend: str, glob_path: Optional[str], budget_s: float) -> Tuple[bool, float]:
    env = os.environ.copy()
    env["UNIPANDAS_BACKEND"] = backend
    cmd = [PY, str(SCRIPT)]
    if glob_path:
        cmd += ["--data-glob", glob_path]
    cmd += ["--only-backend", backend, "--materialize", "count", "--no-md"]
    start = __import__("time").perf_counter()
    try:
        subprocess.run(cmd, env=env, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, timeout=budget_s)
        elapsed = __import__("time").perf_counter() - start
        return True, elapsed
    except Exception:
        elapsed = __import__("time").perf_counter() - start
        return False, elapsed


def _baseline_throughput(backend: str, baseline_size: int, budget_s: float, template: Optional[str]) -> Tuple[float, float]:
    glob_path = template.format(size=baseline_size) if template else _preferred_glob(baseline_size)
    ok, elapsed = _run_once(backend, glob_path, budget_s)
    if not ok or elapsed <= 0:
        return 0.0, elapsed
    return float(baseline_size) / elapsed, elapsed


def _maximize_under_budget(backend: str, budget_s: float, template: Optional[str]) -> Tuple[int, float]:
    best_rows = 0
    best_elapsed = 0.0
    for size in SCALES:
        glob_path = template.format(size=size) if template else _preferred_glob(size)
        ok, elapsed = _run_once(backend, glob_path, budget_s)
        if ok:
            best_rows, best_elapsed = size, elapsed
        else:
            break
    return best_rows, best_elapsed


def main() -> int:
    p = argparse.ArgumentParser(description="Fastest backends: 3-minute groupby capacity")
    p.add_argument("--budget", type=float, default=180.0, help="Seconds per backend")
    p.add_argument("--top-n", type=int, default=3, help="Select top N fastest backends by baseline throughput")
    p.add_argument("--baseline-size", type=int, default=1_000_000, help="Rows for baseline throughput measurement")
    p.add_argument("--data-glob-template", default=None, help="Optional '{size}' template for parquet glob")
    args = p.parse_args()

    baseline_rows = args.baseline_size
    template = args.data_glob_template

    # Phase 1: baseline throughput for each backend
    stats: List[Tuple[str, float, float]] = []  # (backend, throughput rows/s, elapsed)
    for backend in Backends:
        thr, elapsed = _baseline_throughput(backend, baseline_rows, min(args.budget, 60.0), template)
        stats.append((backend, thr, elapsed))

    # Select top N backends with positive throughput
    selected = [b for b, _, _ in sorted(stats, key=lambda t: t[1], reverse=True) if _ > 0][: max(1, args.top_n)]

    headers = ["backend", "baseline_rows", "baseline_s", "selected", "max_rows_within_3min", "elapsed_s_at_max"]
    rows_out: List[List[str]] = []

    # Phase 2: maximize rows within the budget
    for backend, thr, elapsed in stats:
        if backend in selected:
            best_rows, best_elapsed = _maximize_under_budget(backend, args.budget, template)
            rows_out.append([
                backend,
                f"{baseline_rows}",
                f"{elapsed:.3f}" if elapsed > 0 else "-",
                "yes",
                f"{best_rows:.1e}",
                f"{best_elapsed:.3f}" if best_elapsed > 0 else "-",
            ])
        else:
            rows_out.append([
                backend,
                f"{baseline_rows}",
                f"{elapsed:.3f}" if elapsed > 0 else "-",
                "no",
                "-",
                "-",
            ])

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        from mdreport import Report  # type: ignore
    except Exception:
        # Minimal fallback writer
        widths = [max(len(headers[i]), max((len(r[i]) for r in rows_out), default=0)) for i in range(len(headers))]
        def fmt_row(vals: List[str]) -> str:
            return "  ".join(v.ljust(widths[i]) if i < 1 else v.rjust(widths[i]) for i, v in enumerate(vals))
        lines = ["# Fast 3-minute BRC (groupby)", "", f"Generated at: {ts}", "", "```text", fmt_row(headers), "  ".join("-" * w for w in widths)]
        for r in rows_out:
            lines.append(fmt_row(r))
        lines += ["```", ""]
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text("\n".join(lines))
        return 0

    rpt = Report(OUT)
    rpt.title("Fast 3-minute BRC (groupby)").preface([f"Generated at: {ts}", ""]) \
       .table(headers, rows_out, align_from=2, style="fixed").write()
    return 0


if __name__ == "__main__":
    sys.exit(main())
