#!/usr/bin/env python3

from __future__ import annotations

"""03 - No mercy: require every backend to process 1 billion rows (groupby).

Runs the canonical challenge for each backend on the 1B Parquet scale when
available. Does not enforce a timeout. Records success and timings.
"""

import sys
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import List, Optional

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.brc.brc_paths import REPORTS_BRC as REPORTS, REPORT_MAIN as DEFAULT_OUT  # type: ignore
from scripts.utils import Backends as Backends  # type: ignore

PY = sys.executable or "python3"
SCRIPT = ROOT / "scripts" / "brc" / "billion_row_challenge.py"
ONE_B_GLOB = str(ROOT / "data" / "brc_scales" / "parquet_1000000000" / "*.parquet")


def _has_files(glob_path: str) -> bool:
    import glob
    return bool(glob.glob(glob_path))


def _run_backend(backend: str, out_md: Path) -> List[str]:
    env = os.environ.copy()
    env["UNIPANDAS_BACKEND"] = backend
    if not _has_files(ONE_B_GLOB):
        return [backend, "-", "-", "no"]
    cmd = [
        PY,
        str(SCRIPT),
        "--data-glob",
        ONE_B_GLOB,
        "--only-backend",
        backend,
        "--materialize",
        "count",
        "--md-out",
        str(out_md),
    ]
    try:
        subprocess.run(cmd, env=env, check=True)
        # parse last timings from the out_md
        rs, cs = _parse_last_timings(out_md, backend)
        return [backend, f"{rs:.4f}" if rs is not None else "-", f"{cs:.4f}" if cs is not None else "-", "yes"]
    except Exception:
        return [backend, "-", "-", "no"]


def _parse_last_timings(md_path: Path, backend: str) -> tuple[Optional[float], Optional[float]]:
    try:
        lines = md_path.read_text().splitlines()[::-1]
        for ln in lines:
            if ln.startswith(backend + " "):
                parts = ln.split()
                if len(parts) >= 5:
                    return float(parts[3]), float(parts[4])
        return None, None
    except Exception:
        return None, None


def main() -> int:
    headers = ["backend", "read_s", "compute_s", "ok"]
    rows: List[List[str]] = []
    out_md = REPORTS / "brc_1b_groupby.md"
    out_md.parent.mkdir(parents=True, exist_ok=True)

    for backend in Backends:
        rows.append(_run_backend(backend, out_md))

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        from mdreport import Report  # type: ignore
    except Exception:
        widths = [max(len(headers[i]), max((len(r[i]) for r in rows), default=0)) for i in range(len(headers))]
        def fmt_row(vals: List[str]) -> str:
            return "  ".join(v.ljust(widths[i]) if i < 1 else v.rjust(widths[i]) for i, v in enumerate(vals))
        lines = ["# No mercy: 1B groupby", "", f"Generated at: {ts}", "", "```text", fmt_row(headers), "  ".join("-" * w for w in widths)]
        for r in rows:
            lines.append(fmt_row(r))
        lines += ["```", ""]
        (REPORTS / "brc_no_mercy.md").write_text("\n".join(lines))
        return 0

    rpt = Report(REPORTS / "brc_no_mercy.md")
    rpt.title("No mercy: 1B groupby").preface([f"Generated at: {ts}", ""]) \
       .table(headers, rows, align_from=1, style="fixed").write()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
