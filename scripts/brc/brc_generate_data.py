#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import random
from pathlib import Path
import pandas as pd  # type: ignore


def generate_chunks(out_dir: Path, rows_per_chunk: int, num_chunks: int, seed: int = 123, file_format: str = "parquet") -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    random.seed(seed)
    for i in range(num_chunks):
        suffix = ".parquet" if file_format == "parquet" else ".csv"
        p = out_dir / f"brc_{i:04d}{suffix}"
        if p.exists():
            continue
        base = i * rows_per_chunk
        ids = range(base, base + rows_per_chunk)
        xs = [random.randint(-1000, 1000) for _ in range(rows_per_chunk)]
        ys = [random.randint(-1000, 1000) for _ in range(rows_per_chunk)]
        cs = [random.choice(["x", "y", "z"]) for _ in range(rows_per_chunk)]
        df = pd.DataFrame({"id": list(ids), "x": xs, "y": ys, "cat": cs})
        if file_format == "parquet":
            df.to_parquet(p, index=False)
        else:
            df.to_csv(p, index=False)


def main():
    parser = argparse.ArgumentParser(description="Generate chunked CSVs for BRC scaffold")
    parser.add_argument("--out-dir", default="data/brc", help="Output directory for chunks")
    parser.add_argument("--rows-per-chunk", type=int, default=1_000_000)
    parser.add_argument("--num-chunks", type=int, default=1)
    parser.add_argument("--format", choices=["parquet", "csv"], default="parquet", help="Output file format")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    generate_chunks(out_dir, rows_per_chunk=args.rows_per_chunk, num_chunks=args.num_chunks, file_format=args.format)
    print("Generated chunks under", out_dir)


if __name__ == "__main__":
    main()


