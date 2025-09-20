#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path


def main() -> int:
    p = argparse.ArgumentParser(description="Run BRC JSONL-first and render Markdown via mdreport")
    p.add_argument("--data-glob", required=True, help="Parquet glob")
    p.add_argument("--jsonl-out", default="results/brc.jsonl")
    p.add_argument("--md-out", default="reports/brc/billion_row_challenge.md")
    p.add_argument("--backend", default=None, help="Optional UNIPANDAS_BACKEND override")
    p.add_argument("--materialize", default="count", choices=["head", "count", "all"], help="Materialization mode")
    args = p.parse_args()

    env = os.environ.copy()
    if args.backend:
        env["UNIPANDAS_BACKEND"] = args.backend
    # Ensure directories
    Path(args.jsonl_out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.md_out).parent.mkdir(parents=True, exist_ok=True)

    # 1) Run BRC to JSONL only
    cmd1 = [
        "python",
        "scripts/brc/billion_row_challenge.py",
        "--data-glob",
        args.data_glob,
        "--jsonl-out",
        args.jsonl_out,
        "--no-md",
        "--materialize",
        args.materialize,
    ]
    subprocess.run(cmd1, check=True, env=env)

    # 2) Render Markdown from JSONL (main or smoke determined inside reporter/challenge logic)
    cmd2 = [
        "python",
        "scripts/reports/brc_report_from_jsonl.py",
        "--in",
        args.jsonl_out,
        "--out",
        args.md_out,
    ]
    subprocess.run(cmd2, check=True, env=env)
    print("Wrote", args.md_out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
