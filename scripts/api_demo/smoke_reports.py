#!/usr/bin/env python3

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
PY = sys.executable or "python3"


def run_cmd(cmd: list[str], env: dict[str, str] | None = None) -> None:
    e = os.environ.copy()
    if env:
        e.update(env)
    print("$", " ".join(cmd))
    subprocess.run(cmd, check=True, env=e)


def main() -> int:
    reports = ROOT / "reports"
    # Create subfolders: api_demo/, brc/
    (reports / "api_demo").mkdir(parents=True, exist_ok=True)
    (reports / "brc").mkdir(parents=True, exist_ok=True)

    # 1) Bench backends example
    bench = ROOT / "scripts" / "api_demo" / "bench_backends.py"
    if bench.exists():
        data = ROOT / "data"
        data.mkdir(exist_ok=True)
        sample = data / "smoke.csv"
        if not sample.exists():
            import csv

            with sample.open("w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["a", "b", "cat"])  # columns used by the bench
                w.writerow([1, 2, "x"])
                w.writerow([-1, 5, "y"])
                w.writerow([3, -2, "x"])
        run_cmd([
            PY,
            str(bench),
            str(sample),
            "--assign",
            "--query",
            "a > 0",
            "--groupby",
            "cat",
            "--md-out",
            str(reports / "api_demo" / "api_demo_smoke.md"),
        ])

    # 2) Compatibility matrix
    compat = ROOT / "scripts" / "api_demo" / "compat_matrix.py"
    if compat.exists():
        run_cmd([PY, str(compat), "--md-out", str(reports / "api_demo" / "compatibility.md")])

    # 3) Relational benchmark
    rel = ROOT / "scripts" / "api_demo" / "relational_bench.py"
    if rel.exists():
        run_cmd([PY, str(rel), "--md-out", str(reports / "api_demo" / "relational_api_demo.md")])

    # 4) BRC core (small)
    brc = ROOT / "scripts" / "brc" / "billion_row_challenge.py"
    if brc.exists():
        run_cmd([PY, str(brc), "--rows-per-chunk", "1000", "--num-chunks", "1", "--operation", "filter"])

    # 5) BRC one-minute (tiny check)
    om1 = ROOT / "scripts" / "brc" / "brc_one_minute_runner.py"
    if om1.exists():
        run_cmd([PY, str(om1), "--budget", "10", "--operation", "filter"])  # short budget

    # 6) BRC order-of-magnitude (reduced)
    om = ROOT / "scripts" / "brc" / "billion_row_om_runner.py"
    if om.exists():
        run_cmd([PY, str(om), "--budgets", "10"])  # short budget per step

    print("Smoke reports generated under", reports)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
