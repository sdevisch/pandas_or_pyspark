#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
SCRIPTS = ROOT / "scripts"
BRC = SCRIPTS / "brc"
REPORTS = ROOT / "reports"
RESULTS = ROOT / "results"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# Centralized paths
from scripts.brc.brc_paths import (
    BRC_SCALES,
    RESULTS_BRC_MAIN_JSONL,
    RESULTS_BRC_SMOKE_JSONL,
    REPORT_MAIN,
    REPORT_SMOKE,
)  # type: ignore


def _run(cmd: list[str], env: dict[str, str]) -> None:
    print("\n==>", " ".join(cmd))
    r = subprocess.run(cmd, env=env)
    if r.returncode != 0:
        raise SystemExit(r.returncode)


def main() -> None:
    os.makedirs(REPORTS, exist_ok=True)
    os.makedirs(RESULTS, exist_ok=True)

    env = os.environ.copy()
    env.setdefault("PYARROW_IGNORE_TIMEZONE", "1")

    # Choose dataset globs; prefer 1B if present, else fallback to smaller scale
    glob_1b = str(BRC_SCALES / "parquet_1000000000" / "*.parquet")
    glob_100m = str(BRC_SCALES / "parquet_100000000" / "*.parquet")

    data_glob = glob_1b if list((BRC_SCALES / "parquet_1000000000").glob("*.parquet")) else glob_100m

    # 1) Measure and write JSONL via challenge for the selected scale
    _run([
        sys.executable,
        str(BRC / "billion_row_challenge.py"),
        "--data-glob", data_glob,
        "--jsonl-out", str(RESULTS_BRC_MAIN_JSONL if "1000000000" in data_glob else RESULTS_BRC_SMOKE_JSONL),
        "--no-md",
    ], env)

    # 2) Render BRC report from JSONL
    _run([
        sys.executable,
        str(SCRIPTS / "reports" / "brc_report_from_jsonl.py"),
        "--in", str(RESULTS_BRC_MAIN_JSONL if "1000000000" in data_glob else RESULTS_BRC_SMOKE_JSONL),
        "--out", str(REPORT_MAIN if "1000000000" in data_glob else REPORT_SMOKE),
    ], env)

    # 3) Optional: perf matrix end-to-end quick pass on a subset
    # Generate perf.jsonl with a few frontends/backends to populate matrix
    _run([
        sys.executable,
        str(SCRIPTS / "perf" / "measure.py"),
        "--frontends", "pandas",
        "--backends", "pandas,duckdb,polars",
        "--glob", data_glob,
        "--out", str(ROOT / "results" / "perf.jsonl"),
        "--append",
    ], env)

    _run([
        sys.executable,
        str(SCRIPTS / "reports" / "perf_matrix.py"),
        "--in", str(ROOT / "results" / "perf.jsonl"),
        "--out", str(ROOT / "reports" / "perf" / "perf_matrix.md"),
    ], env)

    print("\nCompleted large-data perf run at:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


if __name__ == "__main__":
    main()
