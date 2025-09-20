from __future__ import annotations

import os
import sys
import subprocess
from pathlib import Path

import pandas as pd  # type: ignore
import pytest


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _ensure_tiny_parquet(tmpdir: Path) -> str:
    tmpdir.mkdir(parents=True, exist_ok=True)
    p = tmpdir / "tiny.parquet"
    if not p.exists():
        df = pd.DataFrame({
            "id": [0, 1, 2, 3],
            "x": [1, -1, 2, 0],
            "y": [0, -2, 5, 1],
            "cat": ["x", "y", "x", "z"],
        })
        df.to_parquet(p, index=False)
    return str(tmpdir / "*.parquet")


def _ensure_tiny_csv(tmpdir: Path) -> str:
    tmpdir.mkdir(parents=True, exist_ok=True)
    p = tmpdir / "tiny.csv"
    if not p.exists():
        df = pd.DataFrame({"a": [1, 2, -1], "b": [10, -5, 0], "cat": ["x", "y", "x"]})
        df.to_csv(p, index=False)
    return str(p)


@pytest.mark.timeout(90)
def test_run_core_scripts(tmp_path: Path):
    root = _repo_root()
    env = os.environ.copy()
    env.setdefault("PYARROW_IGNORE_TIMEZONE", "1")

    # 1) perf measure + matrix from tiny parquet
    tiny_parquet_glob = _ensure_tiny_parquet(tmp_path / "parq")
    measure = root / "scripts/perf/measure.py"
    matrix = root / "scripts/reports/perf_matrix.py"
    if measure.exists():
        r = subprocess.run(
            [sys.executable, str(measure), "--frontends", "pandas", "--backends", "pandas,polars,duckdb", "--glob", tiny_parquet_glob, "--out", str(root / "results/test_perf.jsonl")],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=60,
        )
        assert r.returncode == 0, r.stdout.decode()
    if matrix.exists():
        r = subprocess.run(
            [sys.executable, str(matrix), "--in", str(root / "results/test_perf.jsonl"), "--out", str(root / "reports/perf/test_perf_matrix.md")],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=30,
        )
        assert r.returncode == 0, r.stdout.decode()

    # 2) BRC JSONL pipeline on tiny parquet (updates smoke)
    pipeline = root / "scripts/brc/brc_jsonl_pipeline.py"
    if pipeline.exists():
        r = subprocess.run(
            [sys.executable, str(pipeline), "--data-glob", tiny_parquet_glob, "--jsonl-out", str(root / "results/test_brc.jsonl"), "--md-out", str(root / "reports/brc/test_brc_smoke.md")],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=60,
        )
        assert r.returncode == 0, r.stdout.decode()

    # 3) Bench backends on tiny CSV (if present)
    bench = root / "scripts/api_demo/bench_backends.py"
    if bench.exists():
        tiny_csv = _ensure_tiny_csv(tmp_path / "csv")
        r = subprocess.run(
            [sys.executable, str(bench), tiny_csv, "--md-out", str(root / "reports/api_demo/test_bench.md")],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=60,
        )
        assert r.returncode == 0, r.stdout.decode()
