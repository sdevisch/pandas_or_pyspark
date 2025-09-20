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
REPORTS = ROOT / "reports"
RESULTS = ROOT / "results"

# Ensure repo root on path for script execution
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# Define required paths locally (avoid relying on scripts.brc.brc_paths exports)
DATA = ROOT / "data"
BRC_SCALES = DATA / "brc_scales"

REPORTS_BRC = REPORTS / "brc"
REPORTS_PERF = REPORTS / "perf"
REPORTS_BRC.mkdir(parents=True, exist_ok=True)
REPORTS_PERF.mkdir(parents=True, exist_ok=True)

RESULTS_BRC = RESULTS / "brc"
RESULTS_BRC.mkdir(parents=True, exist_ok=True)

REPORT_MAIN = REPORTS_BRC / "brc_1b_groupby.md"
REPORT_SMOKE = REPORTS_BRC / "brc_smoke_groupby.md"

RESULTS_BRC_MAIN_JSONL = RESULTS_BRC / "brc_1b_groupby.jsonl"
RESULTS_BRC_SMOKE_JSONL = RESULTS_BRC / "brc_smoke_groupby.jsonl"


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
    glob_1b_dir = BRC_SCALES / "parquet_1000000000"
    glob_100m_dir = BRC_SCALES / "parquet_100000000"
    glob_1b = str(glob_1b_dir / "*.parquet")
    glob_100m = str(glob_100m_dir / "*.parquet")

    data_glob = glob_1b if list(glob_1b_dir.glob("*.parquet")) else glob_100m

    # 1) Measure and write JSONL via challenge for the selected scale
    _run([
        sys.executable,
        str(SCRIPTS / "brc" / "billion_row_challenge.py"),
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
