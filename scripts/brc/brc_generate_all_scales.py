#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
GEN = ROOT / "scripts" / "brc_generate_data.py"

SCALES = [1_000_000, 10_000_000, 100_000_000, 1_000_000_000]


def main():
    parser = argparse.ArgumentParser(description="Generate BRC datasets at 1M/10M/100M/1B (Parquet by default)")
    parser.add_argument("--format", choices=["parquet", "csv"], default="parquet")
    parser.add_argument("--out-root", default="data/brc_scales", help="Root directory for scale datasets")
    parser.add_argument("--chunk-size", type=int, default=1_000_000, help="Rows per file (splits large scales into many files)")
    args = parser.parse_args()

    for rows in SCALES:
        out_dir = Path(args.out_root) / f"{args.format}_{rows}"
        rows_per_chunk = max(1, min(args.chunk_size, rows))
        # Compute number of chunks to cover the scale with rows_per_chunk each
        num_chunks = (rows + rows_per_chunk - 1) // rows_per_chunk
        cmd = [
            sys.executable or "python3",
            str(GEN),
            "--out-dir",
            str(out_dir),
            "--rows-per-chunk",
            str(rows_per_chunk),
            "--num-chunks",
            str(num_chunks),
            "--format",
            args.format,
        ]
        print("$", " ".join(cmd))
        subprocess.run(cmd, check=True)
    print("Done.")


if __name__ == "__main__":
    main()


