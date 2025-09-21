from __future__ import annotations

"""Runner for the 3-minute unipandas API demo flow.

This module encapsulates all non-orchestration work for the demo: data
preparation, invoking sub-scripts, and writing the aggregated Markdown
report. The CLI entrypoint should only import `parse_args` and call
`run_demo_flow`.
"""

import os
import sys
import time
import platform
from pathlib import Path
from datetime import datetime
from typing import List, Optional


ROOT = Path(__file__).resolve().parents[2]
REPORTS = ROOT / "reports" / "api_demo"
REPORTS.mkdir(parents=True, exist_ok=True)

BENCH = ROOT / "scripts" / "api_demo" / "bench_backends.py"
COMPAT = ROOT / "scripts" / "api_demo" / "compat_matrix.py"
REL = ROOT / "scripts" / "api_demo" / "relational_bench.py"
BRC = ROOT / "scripts" / "brc" / "billion_row_challenge.py"
BRC_OM = ROOT / "scripts" / "brc" / "billion_row_om_runner.py"
BRC_1M = ROOT / "scripts" / "brc" / "brc_one_minute_runner.py"
PIPELINE = ROOT / "scripts" / "brc" / "brc_jsonl_pipeline.py"

PY = sys.executable or "python3"

# Centralized tiny data helpers
from scripts.data_gen.demo_data import ensure_smoke_csv, ensure_tiny_parquet_glob  # type: ignore

def _smoke_csv() -> Path:
    return ensure_smoke_csv(ROOT)


def _tiny_parquet_glob(rows: int = 1000) -> str:
    return ensure_tiny_parquet_glob(ROOT, rows)


def _append_lines(report_path: Path, lines: List[str]) -> None:
    with report_path.open("a") as f:
        f.write("\n".join(lines) + "\n")


def _append_section(report_path: Path, title: str, md_part: Path) -> None:
    with report_path.open("a") as f:
        f.write(f"\n## {title}\n\n")
        f.write(md_part.read_text())


def _run(command: List[str]) -> None:
    import subprocess as _sp

    _sp.run(command, check=True)


def _run_with_timeout(command: List[str], timeout_s: float) -> bool:
    import subprocess as _sp

    try:
        _sp.run(command, check=True, timeout=timeout_s)
        return True
    except _sp.TimeoutExpired:
        return False


def _init_report(path: Path) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with path.open("w") as f:
        f.write("# unipandas API demos\n\n")
        f.write(f"Generated at: {ts}\n\n")
        f.write(f"- Python: `{platform.python_version()}` on `{platform.platform()}`\n")
        f.write(f"- CPU cores: {os.cpu_count()}\n\n")


def _deadline_passed(deadline: float) -> bool:
    return time.perf_counter() >= deadline


def _remaining(deadline: float, cap: float) -> float:
    return min(cap, max(1.0, deadline - time.perf_counter()))


def _maybe_bench(report: Path, sample: Path, deadline: float) -> None:
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
    if _deadline_passed(deadline):
        return
    frag = REPORTS / "compatibility.md"
    _run([PY, str(COMPAT), "--md-out", str(frag)])
    _append_section(report, "Compatibility matrix", frag)
    _append_lines(report, [f"- Completed compat_matrix at {time.strftime('%H:%M:%S')}\n"])


def _maybe_rel(report: Path, deadline: float) -> None:
    if _deadline_passed(deadline):
        return
    frag = REPORTS / "relational_api_demo.md"
    _run([PY, str(REL), "--md-out", str(frag)])
    _append_section(report, "Relational API demos", frag)
    _append_lines(report, [f"- Completed relational_bench at {time.strftime('%H:%M:%S')}\n"])


def _maybe_brc_smoke(report: Path, deadline: float) -> None:
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


def _maybe_brc_one_minute(deadline: float) -> None:
    if _deadline_passed(deadline):
        return
    _run_with_timeout([PY, str(BRC_1M), "--budget", "30"], _remaining(deadline, 30.0))


def _maybe_brc_om(deadline: float) -> None:
    if _deadline_passed(deadline):
        return
    _run_with_timeout([PY, str(BRC_OM), "--budgets", "30"], _remaining(deadline, 30.0))


def run_demo_flow(budget_s: float, out_path: Optional[str] = None) -> int:
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
    # 5) BRC one-minute runner
    _maybe_brc_one_minute(deadline)
    # 6) BRC OM runner
    _maybe_brc_om(deadline)
    return 0


