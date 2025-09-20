from __future__ import annotations

from pathlib import Path
import sys
import subprocess
from typing import Optional

# Ensure project roots are importable
ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
SCRIPTS = ROOT / "scripts"
for p in (SRC, SCRIPTS):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from console.demo import echo_command, echo_timeout, note  # type: ignore


def _run(cmd: list[str]) -> None:
    echo_command(cmd)
    subprocess.run(cmd, check=True)


def _run_timeout(cmd: list[str], timeout_s: float) -> bool:
    echo_command(cmd)
    try:
        subprocess.run(cmd, check=True, timeout=timeout_s)
        return True
    except subprocess.TimeoutExpired:
        echo_timeout(cmd, timeout_s)
        return False


def _ensure_smoke_csv() -> Path:
    data = ROOT / "data"
    data.mkdir(exist_ok=True)
    sample = data / "smoke.csv"
    if sample.exists():
        return sample
    import csv
    with sample.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["a", "b", "cat"])  # columns used by the bench
        w.writerow([1, 2, "x"])
        w.writerow([-1, 5, "y"])
        w.writerow([3, -2, "x"])
    return sample


def _ensure_tiny_parquet(rows: int = 1000) -> str:
    d = ROOT / "data" / f"brc_tmp_{rows}"
    p = d / "generated.parquet"
    if not p.exists():
        import pandas as _pd
        import random as _rnd
        d.mkdir(parents=True, exist_ok=True)
        rng = _rnd.Random(42)
        pdf = _pd.DataFrame({
            "id": list(range(rows)),
            "x": [rng.randint(-1000, 1000) for _ in range(rows)],
            "y": [rng.randint(-1000, 1000) for _ in range(rows)],
            "cat": [rng.choice(["x", "y", "z"]) for _ in range(rows)],
        })
        pdf.to_parquet(str(p), index=False)
    return str(d / "*.parquet")


def run_demo_flow() -> None:
    """Run the 3-minute demo by invoking existing scripts with small inputs."""
    PY = sys.executable or "python3"
    sample = _ensure_smoke_csv()
    tiny_glob = _ensure_tiny_parquet(1000)

    # API bench → reports/api_demo/api_demo_smoke.md
    _run([PY, str(ROOT / "scripts/api_demo/bench_backends.py"), str(sample), "--assign", "--query", "a > 0", "--groupby", "cat", "--md-out", str(ROOT / "reports/api_demo/api_demo_smoke.md")])

    # Compatibility matrix
    _run([PY, str(ROOT / "scripts/api_demo/compat_matrix.py"), "--md-out", str(ROOT / "reports/api_demo/compatibility.md")])

    # Relational demos
    _run([PY, str(ROOT / "scripts/api_demo/relational_bench.py"), "--md-out", str(ROOT / "reports/api_demo/relational_api_demo.md")])

    # BRC JSONL pipeline (smoke)
    _run([PY, str(ROOT / "scripts/brc/brc_jsonl_pipeline.py"), "--data-glob", tiny_glob, "--jsonl-out", str(ROOT / "results/demo_brc.jsonl"), "--md-out", str(ROOT / "reports/brc/test_demo_smoke.md")])

    # One-minute runner and OM runner (best-effort within short budget)
    _run_timeout([PY, str(ROOT / "scripts/brc/brc_one_minute_runner.py"), "--budget", "30"], timeout_s=30.0)
    _run_timeout([PY, str(ROOT / "scripts/brc/billion_row_om_runner.py"), "--budgets", "30"], timeout_s=30.0)

    note("Completed 3-minute demo flow.")


