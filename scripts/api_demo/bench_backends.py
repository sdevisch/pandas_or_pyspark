#!/usr/bin/env python3
"""Benchmark unipandas across backends using shared helpers and perfcore."""
import sys
from typing import List, Optional
import argparse
from pathlib import Path

# This script defers backend specifics to shared helpers in bench_lib and reports/helpers

# Ensure src/ on path to import perfcore
_ROOT = Path(__file__).resolve().parents[2]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

# No direct perfcore usage here; bench_lib handles JSONL writing

# reporting helpers
_HELPERS = Path(__file__).resolve().parents[1] / "reports"
if str(_HELPERS) not in sys.path:
    sys.path.insert(0, str(_HELPERS))
try:
    from helpers import render_bench_markdown, fixed_table  # type: ignore
except Exception:
    render_bench_markdown = None  # type: ignore

# Import shared bench library helpers
_SCRIPTS = Path(__file__).resolve().parents[1]
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))
try:
    from scripts.api_demo.bench_lib import (  # type: ignore
        run_backends as run_benchmarks,
        build_availability as get_availability,
        rows_for_console as build_rows,
        log_results_jsonl as write_jsonl_results,
    )
except Exception:
    # Fallback when running as a module from scripts
    from api_demo.bench_lib import (  # type: ignore
        run_backends as run_benchmarks,
        build_availability as get_availability,
        rows_for_console as build_rows,
        log_results_jsonl as write_jsonl_results,
    )

try:
    from scripts.api_demo.bench_args import parse_args, MATERIALIZE_ROWS_DEFAULT  # type: ignore
except Exception:
    from bench_args import parse_args, MATERIALIZE_ROWS_DEFAULT  # type: ignore


def _format_fixed_width_table(headers: List[str], rows: List[List[str]], right_align_from: int = 2) -> List[str]:
    return fixed_table(headers, rows, right_align_from=right_align_from)


def _used_cores_for_backend(backend: str) -> Optional[int]:
    return None


def _count_rows_backend(df) -> int:
    return 0


def run_backends(path: str, query: Optional[str], assign: bool, groupby: Optional[str], materialize_rows: int = MATERIALIZE_ROWS_DEFAULT):
    return run_benchmarks(path, query, assign, groupby, materialize_rows)


def log_results(results, jsonl_out: Optional[str]) -> None:
    write_jsonl_results(results, jsonl_out)


def _parse_args():
    return parse_args()


def _availability() -> List[dict]:
    return get_availability()


def _rows_for_console(results, availability):
    return build_rows(results, availability)


def render_markdown(results, args: argparse.Namespace, md_out: Optional[str]) -> None:
    if not md_out:
        return
    availability = get_availability()
    rows = build_rows(results, availability)
    headers = ["backend", "version", "load_s", "compute_s", "input_rows", "used_cores"]
    if render_bench_markdown is not None:
        render_bench_markdown(md_out, args, availability, headers, rows)
    else:
        # Fallback inline minimal path
        content = ["```text", *_format_fixed_width_table(headers, rows), "```", ""]
        with open(md_out, "w") as f:
            f.write("\n".join(content))
    print(f"\nWrote Markdown results to {md_out}")


def _maybe_synthesize_data(args: argparse.Namespace) -> None:
    return


def _print_console_summary(rows: List[List[str]]) -> None:
    headers = ["backend", "version", "load_s", "compute_s", "input_rows", "used_cores"]
    for line in _format_fixed_width_table(headers, rows):
        print(line)


def main() -> int:
    args = _parse_args()
    _maybe_synthesize_data(args)

    print("Backends:", "(see scripts/utils.py)")
    results = run_benchmarks(
        args.path,
        query=args.query,
        assign=args.assign,
        groupby=args.groupby,
        materialize_rows=args.materialize_rows,
    )
    if not results:
        print("No backends produced results.")
        return 1
    rows = _rows_for_console(results, _availability())
    _print_console_summary(rows)
    write_jsonl_results(results, args.jsonl_out)
    render_markdown(results, args, args.md_out)
    return 0


if __name__ == "__main__":
    sys.exit(main())


