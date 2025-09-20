from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd  # type: ignore


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _import_bench_lib():
    # Import bench_lib directly for running benchmarks without console/markdown
    scripts_api_demo = _repo_root() / "scripts" / "api_demo"
    if str(scripts_api_demo) not in sys.path:
        sys.path.insert(0, str(scripts_api_demo))
    import bench_lib as bl  # type: ignore
    return bl


def _make_tiny_csv(tmp_path: Path) -> str:
    p = tmp_path / "tiny.csv"
    df = pd.DataFrame({
        "a": [1, -1, 2],
        "b": [5, 5, -3],
        "cat": ["x", "y", "x"],
    })
    df.to_csv(p, index=False)
    return str(p)


def test_run_backends_minimal(tmp_path: Path):
    bl = _import_bench_lib()
    # Limit to pandas for fast, deterministic test
    bl.Backends = ["pandas"]
    csv_path = _make_tiny_csv(tmp_path)

    results = bl.run_backends(csv_path, query=None, assign=False, groupby="cat", materialize_rows=1000)
    assert results, "Expected at least one result"
    r = results[0]
    # PerfResult.now fields
    assert getattr(r, "backend", None) == "pandas"
    assert isinstance(getattr(r, "read_seconds", None), float)
    assert isinstance(getattr(r, "compute_seconds", None), float)
    assert getattr(r, "input_rows", None) in (3, "3")


def test_log_results_jsonl(tmp_path: Path):
    bl = _import_bench_lib()
    bl.Backends = ["pandas"]
    csv_path = _make_tiny_csv(tmp_path)

    results = bl.run_backends(csv_path, query=None, assign=False, groupby="cat", materialize_rows=1000)
    out = tmp_path / "bench.jsonl"
    bl.log_results_jsonl(results, str(out))
    assert out.exists() and out.stat().st_size > 0


def test_render_markdown(tmp_path: Path):
    bl = _import_bench_lib()
    bl.Backends = ["pandas"]
    csv_path = _make_tiny_csv(tmp_path)

    results = bl.run_backends(csv_path, query=None, assign=False, groupby="cat", materialize_rows=1000)
    md_out = tmp_path / "bench.md"
    args = SimpleNamespace(path=csv_path, assign=False, query=None, groupby="cat")
    # Render via mdreport to keep reporting outside bench files
    sys.path.insert(0, str((_repo_root() / "src")))
    from mdreport.bench import render_bench_from_results  # type: ignore
    render_bench_from_results(results, args, str(md_out))
    assert md_out.exists() and md_out.read_text().strip() != ""
