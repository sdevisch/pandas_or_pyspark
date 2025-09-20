#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

from perfcore import measure_once, write_results


def main() -> int:
    p = argparse.ArgumentParser(description="Run performance measurements and write JSONL results")
    p.add_argument("--frontends", default="pandas,pyspark,narwhals", help="Comma-separated frontends")
    p.add_argument("--backends", default="pandas,dask,pyspark,polars,duckdb,numpy,numba", help="Comma-separated backends")
    p.add_argument("--glob", required=True, help="Parquet glob for dataset")
    p.add_argument("--out", default="results/perf.jsonl", help="Output JSONL path")
    p.add_argument("--materialize", default="count", choices=["head", "count", "all"])
    args = p.parse_args()

    frontends: List[str] = [s.strip() for s in args.frontends.split(",") if s.strip()]
    backends: List[str] = [s.strip() for s in args.backends.split(",") if s.strip()]
    results = []
    for fe in frontends:
        for be in backends:
            results.append(measure_once(frontend=fe, backend=be, dataset_glob=args.glob, materialize=args.materialize))
    write_results(results, Path(args.out))
    print(f"Wrote {len(results)} results to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


