from __future__ import annotations

"""Runner for the 3-minute unipandas API demo flow.

This module orchestrates the demo flow steps and delegates all
markdown/formatting to `src/mdreport`. It avoids side effects at import
time and keeps helpers small and focused.
"""

import sys
import time
from pathlib import Path
from typing import List, Optional


ROOT = Path(__file__).resolve().parents[2]
REPORTS = ROOT / "reports" / "api_demo"

BENCH = ROOT / "scripts" / "api_demo" / "bench_backends.py"
COMPAT = ROOT / "scripts" / "api_demo" / "compat_matrix.py"
REL = ROOT / "scripts" / "api_demo" / "relational_bench.py"
BRC = ROOT / "scripts" / "brc" / "billion_row_challenge.py"
BR01 = ROOT / "scripts" / "brc" / "01_one_minute_groupby.py"
BR02 = ROOT / "scripts" / "brc" / "02_fast_3min_groupby.py"
PIPELINE = ROOT / "scripts" / "brc" / "brc_jsonl_pipeline.py"

PY = sys.executable or "python3"


def _smoke_csv() -> Path:
    """Return a small CSV sample that exists in the repo.

    Prefers `data/smoke.csv`, falling back to other bundled small CSVs.
    """
    candidates = [
        ROOT / "data" / "smoke.csv",
        ROOT / "scripts" / "data" / "compat_small.csv",
        ROOT / "data" / "compat_small.csv",
        ROOT / "data" / "example_small.csv",
        ROOT / "data" / "example.csv",
    ]
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError("No small CSV sample found in data directory")


def _tiny_parquet_glob(rows: int = 1000) -> str:
    """Return a parquet glob pointing to a tiny dataset for BRC smoke.

    Tries `data/brc_tmp_<rows>/*.parquet`, then `data/brc_<rows>/*.parquet`,
    finally any single known parquet under `data/gen_smoke` as last resort.
    """
    tmp_dir = ROOT / "data" / f"brc_tmp_{rows}"
    if tmp_dir.exists():
        return str(tmp_dir / "*.parquet")
    scale_dir = ROOT / "data" / f"brc_{rows}"
    if scale_dir.exists():
        return str(scale_dir / "*.parquet")
    gen_smoke = ROOT / "data" / "gen_smoke"
    if gen_smoke.exists():
        return str(gen_smoke / "*.parquet")
    # Fallback to any parquet under data (kept tiny by caller's rows)
    return str(ROOT / "data" / "**" / "*.parquet")


def _append_lines(report_path: Path, lines: List[str]) -> None:
    """Append lines via mdreport helper to keep markdown logic centralized."""
    from mdreport.report import append_lines as md_append_lines  # type: ignore

    md_append_lines(report_path, lines)


def _append_section(report_path: Path, title: str, md_part: Path) -> None:
    """Append a section from a fragment using mdreport helper."""
    from mdreport.report import append_section_from_file  # type: ignore

    append_section_from_file(report_path, title, md_part)


def _run(command: List[str]) -> None:
    """Run a subprocess command and raise on failure."""
    import subprocess as _sp

    _sp.run(command, check=True)


def _run_with_timeout(command: List[str], timeout_s: float) -> bool:
    """Run a command with a timeout; return True if completed, else False."""
    import subprocess as _sp

    try:
        _sp.run(command, check=True, timeout=timeout_s)
        return True
    except _sp.TimeoutExpired:
        return False


def _init_report(path: Path) -> None:
    """Create the top-level demo report using mdreport header helper."""
    from mdreport.report import write_simple_header, system_info  # type: ignore

    preface = list(system_info())
    write_simple_header(path, title="unipandas API demos", preface_lines=preface)


def _deadline_passed(deadline: float) -> bool:
    """Return True if the time budget is exhausted."""
    return time.perf_counter() >= deadline


def _remaining(deadline: float, cap: float) -> float:
    """Compute safe remaining time, capped and lower-bounded for stability."""
    return min(cap, max(1.0, deadline - time.perf_counter()))


def _maybe_bench(report: Path, sample: Path, deadline: float) -> None:
    """Run bench smoke and attach results if budget allows."""
    if _deadline_passed(deadline):
        return
    frag = REPORTS / "api_demo_smoke.md"
    _run([
        PY,
        str(BENCH),
        str(sample),
        "--assign",
        "--query",
        "a > 0",
        "--groupby",
        "cat",
        "--md-out",
        str(frag),
    ])
    _append_section(report, "API demo smoke run", frag)
    _append_lines(report, [f"- Completed bench_backends at {time.strftime('%H:%M:%S')}\n"])


def _maybe_compat(report: Path, deadline: float) -> None:
    """Run compatibility matrix and attach section if within budget."""
    if _deadline_passed(deadline):
        return
    frag = REPORTS / "compatibility.md"
    _run([PY, str(COMPAT), "--md-out", str(frag)])
    _append_section(report, "Compatibility matrix", frag)
    _append_lines(report, [f"- Completed compat_matrix at {time.strftime('%H:%M:%S')}\n"])


def _maybe_rel(report: Path, deadline: float) -> None:
    """Run relational API demo and attach section if within budget."""
    if _deadline_passed(deadline):
        return
    frag = REPORTS / "relational_api_demo.md"
    _run([PY, str(REL), "--md-out", str(frag)])
    _append_section(report, "Relational API demos", frag)
    _append_lines(report, [f"- Completed relational_bench at {time.strftime('%H:%M:%S')}\n"])


def _maybe_brc_smoke(report: Path, deadline: float) -> None:
    """Run a tiny BRC groupby and attach section if finished in time."""
    if _deadline_passed(deadline):
        return
    tiny_glob = _tiny_parquet_glob(1000)
    frag = ROOT / "reports" / "brc" / "test_demo_smoke.md"
    timeout = _remaining(deadline, 30.0)
    if PIPELINE.exists():
        ok = _run_with_timeout([
            PY,
            str(PIPELINE),
            "--data-glob",
            tiny_glob,
            "--jsonl-out",
            str(ROOT / "results" / "demo_brc.jsonl"),
            "--md-out",
            str(frag),
        ], timeout)
    else:
        ok = _run_with_timeout([PY, str(BRC), "--data-glob", tiny_glob], timeout)
    if ok:
        _append_section(report, "BRC smoke (groupby)", frag)
    else:
        _append_lines(report, ["", "## Skipped due to 3-minute budget", "- billion_row_challenge (timeout)", ""])  # minimal note


def _maybe_brc_one_minute(report: Path, deadline: float) -> None:
    """Run the 01_one_minute_groupby script within a 30s slice and attach."""
    if _deadline_passed(deadline):
        return
    _run_with_timeout([PY, str(BR01), "--budget", "30"], _remaining(deadline, 30.0))
    frag = ROOT / "reports" / "brc" / "brc_under_1min_capacity.md"
    if frag.exists():
        _append_section(report, "1-minute BRC capacity (per backend)", frag)


def _maybe_brc_fast_3min(report: Path, deadline: float) -> None:
    """Run the 02_fast_3min_groupby script in a 30s slice and attach."""
    if _deadline_passed(deadline):
        return
    _run_with_timeout([PY, str(BR02), "--budget", "30"], _remaining(deadline, 30.0))
    frag = ROOT / "reports" / "brc" / "brc_fast_3min_groupby.md"
    if frag.exists():
        _append_section(report, "Fast backends: 3-minute capacity", frag)


def run_demo_flow(budget_s: float, out_path: Optional[str] = None) -> int:
    """Run the full 3-minute demo and write an aggregated markdown report.

    - budget_s: total seconds for the end-to-end flow
    - out_path: optional report path; defaults under `reports/api_demo/`
    Returns 0 on success.
    """
    report = Path(out_path) if out_path else (REPORTS / "demo_api_3_min.md")
    _init_report(report)

    sample = _smoke_csv()
    deadline = time.perf_counter() + float(budget_s)
    # 1) Bench backends
    _maybe_bench(report, sample, deadline)
    # 2) Compatibility matrix
    _maybe_compat(report, deadline)
    # 3) Relational API demos
    _maybe_rel(report, deadline)
    # 4) BRC smoke
    _maybe_brc_smoke(report, deadline)
    # 5) 1-minute capacity summary (01)
    _maybe_brc_one_minute(report, deadline)
    # 6) Fast backends 3-minute capacity (02)
    _maybe_brc_fast_3min(report, deadline)
    return 0


