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


def _scripts_to_run(root: Path, tiny_parquet: str, tiny_csv: str):
    runs = []
    mp = root / "scripts/perf/measure.py"
    if mp.exists():
        runs.append(
            (
                mp,
                [
                    "--frontends",
                    "pandas",
                    "--backends",
                    "pandas,polars,duckdb",
                    "--glob",
                    tiny_parquet,
                    "--out",
                    str(root / "results/test_perf.jsonl"),
                ],
            )
        )
    pm = root / "scripts/reports/perf_matrix.py"
    if pm.exists():
        runs.append(
            (
                pm,
                ["--in", str(root / "results/test_perf.jsonl"), "--out", str(root / "reports/perf/test_perf_matrix.md")],
            )
        )
    pipeline = root / "scripts/brc/brc_jsonl_pipeline.py"
    if pipeline.exists():
        runs.append(
            (
                pipeline,
                [
                    "--data-glob",
                    tiny_parquet,
                    "--jsonl-out",
                    str(root / "results/test_brc.jsonl"),
                    "--md-out",
                    str(root / "reports/brc/test_brc_smoke.md"),
                ],
            )
        )
    brc = root / "scripts/brc/billion_row_challenge.py"
    if brc.exists():
        runs.append((brc, ["--data-glob", tiny_parquet, "--only-backend", "pandas", "--no-md"]))
    om = root / "scripts/brc/billion_row_om_runner.py"
    if om.exists():
        runs.append((om, ["--budgets", "5"]))
    one_min = root / "scripts/brc/brc_one_minute_runner.py"
    if one_min.exists():
        runs.append((one_min, ["--budget", "5"]))
    bench = root / "scripts/api_demo/bench_backends.py"
    if bench.exists():
        runs.append((bench, [tiny_csv, "--md-out", str(root / "reports/api_demo/test_bench.md")]))
    compat = root / "scripts/api_demo/compat_matrix.py"
    if compat.exists():
        runs.append((compat, []))
    rel = root / "scripts/api_demo/relational_bench.py"
    if rel.exists():
        runs.append((rel, []))
    # parquet inventory (utility)
    pinv = root / "scripts/parquet_inventory.py"
    if pinv.exists():
        runs.append((pinv, ["--glob", tiny_parquet]))
    # data gen: we will only smoke the light generator if present
    gen = root / "scripts/data_gen/brc_generate_data.py"
    if gen.exists():
        runs.append((gen, ["--rows", "100", "--out", str(root / "data/gen_smoke")]))
    brc_report = root / "scripts/reports/brc_report_from_jsonl.py"
    if brc_report.exists():
        runs.append(
            (
                brc_report,
                ["--in", str(root / "results/test_brc.jsonl"), "--out", str(root / "reports/brc/test_brc_from_jsonl.md")],
            )
        )
    return runs


@pytest.mark.timeout(180)
def test_run_all_scripts(tmp_path: Path):
    root = _repo_root()
    env = os.environ.copy()
    env.setdefault("PYARROW_IGNORE_TIMEZONE", "1")

    tiny_parquet_glob = _ensure_tiny_parquet(tmp_path / "parq")
    tiny_csv = _ensure_tiny_csv(tmp_path / "csv")

    for script, args in _scripts_to_run(root, tiny_parquet_glob, tiny_csv):
        cmd = [sys.executable, str(script), *args]
        r = subprocess.run(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=90)
        assert r.returncode == 0, f"{script} failed with output:\n{r.stdout.decode()}"
