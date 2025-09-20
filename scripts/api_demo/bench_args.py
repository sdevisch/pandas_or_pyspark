from __future__ import annotations

import argparse

MATERIALIZE_ROWS_DEFAULT = 1_000_000


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Benchmark unipandas across backends")
    p.add_argument("path", help="Path to CSV file to read")
    p.add_argument("--query", default=None, help="Optional pandas query string, e.g. 'a > 0'")
    p.add_argument("--assign", action="store_true", help="Add column c = a + b before compute")
    p.add_argument("--groupby", default="cat", help="Groupby column name to count (default: cat)")
    p.add_argument("--code-file", default=None, help="Optional path to the pandas code file processed")
    p.add_argument("--md-out", default=None, help="If set, write results as Markdown to this file")
    p.add_argument("--jsonl-out", default=None, help="If set, write results JSONL via perfcore to this file")
    p.add_argument("--materialize-rows", type=int, default=MATERIALIZE_ROWS_DEFAULT, help="Rows to materialize for compute timing")
    return p.parse_args()
