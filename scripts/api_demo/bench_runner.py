from __future__ import annotations

"""Orchestrator for bench flow: run → console → jsonl → markdown.

This module centralizes all output (console and markdown) away from
bench_backends.py to preserve strict separation of concerns.
"""

import sys
from typing import Any
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
SCRIPTS = ROOT / "scripts"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from scripts.api_demo.bench_lib import run_backends, build_availability, rows_for_console, log_results_jsonl  # type: ignore
from console.bench import print_bench_console_summary  # type: ignore
from mdreport.bench import render_bench_from_results  # type: ignore


def run_bench_flow(args: Any) -> int:
    """Execute the full benchmark flow using shared helpers.

    Returns process exit code.
    """
    results = run_backends(
        args.path,
        query=args.query,
        assign=args.assign,
        groupby=args.groupby,
        materialize_rows=args.materialize_rows,
    )
    if not results:
        print("No backends produced results.")
        return 1

    availability = build_availability()
    rows = rows_for_console(results, availability)
    print_bench_console_summary(rows)

    log_results_jsonl(results, args.jsonl_out)

    if getattr(args, "md_out", None):
        render_bench_from_results(results, args, args.md_out)

    return 0


