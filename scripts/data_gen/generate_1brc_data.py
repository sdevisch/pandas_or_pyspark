#!/usr/bin/env python3

from __future__ import annotations

"""
Generate data in the 1BRC format (measurements.txt).

Each line: "<station>;<temperature>" where temperature is a decimal with one
fractional digit (e.g., 12.3), and station is a short string. This follows the
spirit of the 1BRC dataset described in the challenge: https://github.com/dougmercer-yt/1brc?tab=readme-ov-file

Note: This is a synthetic generator for local testing, not an official dataset.
"""

import argparse
import random
from pathlib import Path
from typing import List


DEFAULT_STATIONS: List[str] = [
    "Amsterdam",
    "Berlin",
    "Copenhagen",
    "Dublin",
    "Edinburgh",
    "Florence",
    "Geneva",
    "Hamburg",
    "Istanbul",
    "Jerusalem",
]


def generate_measurements(out_path: Path, rows: int, stations: List[str], seed: int = 42) -> None:
    random.seed(seed)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for _ in range(rows):
            station = random.choice(stations)
            # temperatures between -50.0 and +50.0 with 0.1 precision
            temp_tenth = random.randint(-500, 500)
            temp_str = f"{temp_tenth/10:.1f}"
            f.write(f"{station};{temp_str}\n")


def main():
    parser = argparse.ArgumentParser(description="Generate 1BRC-style measurements.txt")
    parser.add_argument("--rows", type=int, default=1_000_000, help="Number of rows to generate")
    parser.add_argument("--out", type=str, default="data/measurements.txt", help="Output file path")
    args = parser.parse_args()

    out_path = Path(args.out)
    generate_measurements(out_path, rows=args.rows, stations=DEFAULT_STATIONS)
    print("Wrote", out_path, "rows=", args.rows)


if __name__ == "__main__":
    main()

#/usr/bin/env python3 --version | cat python3
