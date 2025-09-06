#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import random
from pathlib import Path


def generate_chunks(out_dir: Path, rows_per_chunk: int, num_chunks: int, seed: int = 123) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    random.seed(seed)
    for i in range(num_chunks):
        p = out_dir / f"brc_{i:04d}.csv"
        if p.exists():
            continue
        with p.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["id", "x", "y", "cat"])
            base = i * rows_per_chunk
            for j in range(rows_per_chunk):
                rid = base + j
                w.writerow(
                    [
                        rid,
                        random.randint(-1000, 1000),
                        random.randint(-1000, 1000),
                        random.choice(["x", "y", "z"]),
                    ]
                )


def main():
    parser = argparse.ArgumentParser(description="Generate chunked CSVs for BRC scaffold")
    parser.add_argument("--out-dir", default="data/brc", help="Output directory for chunks")
    parser.add_argument("--rows-per-chunk", type=int, default=1_000_000)
    parser.add_argument("--num-chunks", type=int, default=1)
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    generate_chunks(out_dir, rows_per_chunk=args.rows_per_chunk, num_chunks=args.num_chunks)
    print("Generated chunks under", out_dir)


if __name__ == "__main__":
    main()


