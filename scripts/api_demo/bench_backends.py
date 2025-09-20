#!/usr/bin/env python3
"""Benchmark unipandas across backends using shared helpers and perfcore.

This file is intentionally small and reads like a story:
1) parse arguments
2) run benchmarks
3) print a concise console summary
4) write JSONL and, optionally, a Markdown report
"""
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

# reporting helpers (prefer mdreport package)
try:
    from mdreport.bench import render_bench_markdown  # type: ignore
except Exception:
    render_bench_markdown = None  # type: ignore


def fixed_table(headers: List[str], rows: List[List[str]], right_align_from: int = 2) -> List[str]:
    widths = [max(len(headers[i]), max((len(r[i]) for r in rows), default=0)) for i in range(len(headers))]
    def fmt(vals: List[str]) -> str:
        parts: List[str] = []
        for i, v in enumerate(vals):
            parts.append(v.ljust(widths[i]) if i < right_align_from else v.rjust(widths[i]))
        return "  ".join(parts)
    lines: List[str] = [fmt(headers), "  ".join('-' * w for w in widths)]
    for row in rows:
        lines.append(fmt(row))
    return lines

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


# Console headers for readability
HEADERS = ["backend", "version", "load_s", "compute_s", "input_rows", "used_cores"]


def run_backends(path: str, query: Optional[str], assign: bool, groupby: Optional[str], materialize_rows: int = MATERIALIZE_ROWS_DEFAULT):
    return run_benchmarks(path, query, assign, groupby, materialize_rows)


def log_results(results, jsonl_out: Optional[str]) -> None:
    write_jsonl_results(results, jsonl_out)


def get_cli_args() -> argparse.Namespace:
    """Parse and return command-line arguments for the benchmark."""
    return parse_args()


def _availability() -> List[dict]:
    """Return backend availability and versions."""
    return get_availability()


def build_console_rows(results, availability):
    """Build rows for console/markdown, in a consistent backend order."""
    return build_rows(results, availability)


def _render_with_mdreport(results, args: argparse.Namespace, md_out: Optional[str]) -> None:
    if not md_out:
        return
    availability = get_availability()
    rows = build_rows(results, availability)
    if render_bench_markdown is not None:
        render_bench_markdown(md_out, args, availability, HEADERS, rows)
        print(f"\nWrote Markdown results to {md_out}")
    else:
        # Fallback inline minimal path
        content = ["```text", *_format_fixed_width_table(HEADERS, rows), "```", ""]
        with open(md_out, "w") as f:
            f.write("\n".join(content))
        print(f"\nWrote Markdown results to {md_out}")


def render_markdown(results, args: argparse.Namespace, md_out: Optional[str]) -> None:  # backwards-compat for tests
    _render_with_mdreport(results, args, md_out)


def _maybe_synthesize_data(args: argparse.Namespace) -> None:
    return


def _print_console_summary(rows: List[List[str]]) -> None:
    """Print the tabular summary to stdout in a fixed-width format."""
    for line in _format_fixed_width_table(HEADERS, rows):
        print(line)


def _announce_backends() -> None:
    """Explain where the list of backends comes from."""
    print("Backends:", "(see scripts/utils.py)")


def _summarize_and_write(results, args) -> None:
    """Summarize to console, write JSONL, and render Markdown if requested."""
    rows = build_console_rows(results, _availability())
    _print_console_summary(rows)
    write_jsonl_results(results, args.jsonl_out)
    _render_with_mdreport(results, args, args.md_out)


def results_are_empty(results) -> bool:
    """Return True if no results were produced by any backend."""
    return not results


def main() -> int:
    args = get_cli_args()
    _maybe_synthesize_data(args)

    _announce_backends()
    results = run_benchmarks(
        args.path,
        # query: optional pandas-like filter expression applied before aggregation
        query=args.query,
        # assign: when True, adds column c = a + b before subsequent operations
        assign=args.assign,
        # groupby: name of the column to group on (rows are counted per group)
        groupby=args.groupby,
        # materialize_rows: number of rows to materialize to time the compute step
        materialize_rows=args.materialize_rows,
    )
    if results_are_empty(results):
        print("No backends produced results.")
        return 1
    _summarize_and_write(results, args)
    return 0


if __name__ == "__main__":
    sys.exit(main())


