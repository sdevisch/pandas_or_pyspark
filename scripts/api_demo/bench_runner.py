from __future__ import annotations

"""Orchestrator for bench flow: run → console → jsonl → markdown.

This module centralizes all output (console and markdown) away from
bench_backends.py to preserve strict separation of concerns.
"""

import sys
from typing import Any
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parents[1]
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

try:
    from scripts.api_demo.bench_lib import run_backends, build_availability, rows_for_console, log_results_jsonl  # type: ignore
except Exception:
    from api_demo.bench_lib import run_backends, build_availability, rows_for_console, log_results_jsonl  # type: ignore

try:
    from scripts.api_demo.bench_console import print_bench_console_summary  # type: ignore
except Exception:
    from api_demo.bench_console import print_bench_console_summary  # type: ignore

try:
    from mdreport.bench import render_bench_from_results  # type: ignore
except Exception:
    render_bench_from_results = None  # type: ignore


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

    if getattr(args, "md_out", None) and render_bench_from_results is not None:
        render_bench_from_results(results, args, args.md_out)

    return 0


