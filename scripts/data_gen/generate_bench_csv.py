#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import random
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate synthetic CSV for bench_backends.py")
    p.add_argument("--rows", type=int, required=True, help="Number of rows to generate")
    p.add_argument("--out", type=str, required=True, help="Output CSV path")
    p.add_argument("--seed", type=int, default=42, help="Random seed")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    random.seed(args.seed)
    with out_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["a", "b", "cat"])  # header
        for _ in range(args.rows):
            w.writerow([
                random.randint(-1000, 1000),
                random.randint(-1000, 1000),
                random.choice(["x", "y", "z"]),
            ])
    print(f"Wrote {args.rows} rows to {out_path}")


if __name__ == "__main__":
    main()
