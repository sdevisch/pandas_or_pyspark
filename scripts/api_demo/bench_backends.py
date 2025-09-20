#!/usr/bin/env python3
"""Benchmark orchestrator: parse CLI args and delegate to the runner.

Separation of duties:
- This file only parses inputs and calls the dedicated runner.
- Console output lives in src/console/.
- Markdown rendering lives in src/mdreport/.
"""
import sys
import argparse
from pathlib import Path

# Ensure project root on sys.path so absolute imports like `scripts.api_demo.*` work
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts.api_demo.bench_args import parse_args  # type: ignore
from scripts.api_demo.bench_runner import run_bench_flow  # type: ignore


def get_cli_args() -> argparse.Namespace:
    return parse_args()


def main() -> int:
    args = get_cli_args()
    return run_bench_flow(args)


if __name__ == "__main__":
    sys.exit(main())


