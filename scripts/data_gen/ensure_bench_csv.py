#!/usr/bin/env python3

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ensure a benchmark CSV exists; generate if missing.")
    p.add_argument("--out", required=True, help="Target CSV path to ensure")
    p.add_argument("--rows", type=int, default=1_000_000, help="Rows to generate if missing")
    p.add_argument("--seed", type=int, default=42, help="Random seed for generation")
    return p.parse_args()


essential_rel = Path(__file__).resolve().parent / "generate_bench_csv.py"


def main() -> None:
    args = parse_args()
    out = Path(args.out)
    if out.exists():
        print(f"exists: {out}")
        return
    out.parent.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, str(essential_rel), "--rows", str(args.rows), "--out", str(out), "--seed", str(args.seed)]
    print("generate:", " ".join(cmd))
    r = subprocess.run(cmd)
    if r.returncode != 0:
        raise SystemExit(r.returncode)


if __name__ == "__main__":
    main()
