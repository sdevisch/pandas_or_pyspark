#!/usr/bin/env python3
"""Benchmark unipandas across backends with clear separation of duties.

Responsibilities:
1) run_backends: run the benchmark and return results
2) log_results: write results to JSONL via perfcore
3) render_markdown: write a Markdown report (mdreport if available)
"""
import argparse
import os
import sys
import time
from typing import Dict, List, Optional
from datetime import datetime
import platform
from pathlib import Path

from unipandas import configure_backend, read_csv
# Support running as a script or as a module
try:  # when invoked as a module: python -m api_demo.bench_backends
    from .utils import (
        Backends as ALL_BACKENDS,
        get_backend_version as utils_get_backend_version,
        check_available as utils_check_available,
        format_fixed as utils_format_fixed,
        used_cores_for_backend as utils_used_cores_for_backend,
    )
except Exception:  # when invoked as a script: python scripts/api_demo/bench_backends.py
    import sys
    from pathlib import Path

    # Add the scripts directory (which contains utils.py) to sys.path
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from utils import (  # type: ignore
        Backends as ALL_BACKENDS,
        get_backend_version as utils_get_backend_version,
        check_available as utils_check_available,
        format_fixed as utils_format_fixed,
        used_cores_for_backend as utils_used_cores_for_backend,
    )


Backends = ALL_BACKENDS  # Canonical list comes from scripts/utils.py

# Ensure src/ on path to import perfcore
_ROOT = Path(__file__).resolve().parents[2]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

try:
    from perfcore.result import Result as PerfResult, write_results as perf_write_results  # type: ignore
except Exception:
    PerfResult = None  # type: ignore
    perf_write_results = None  # type: ignore


def get_backend_version(backend: str) -> Optional[str]:
    """Return version string for ``backend`` or None if unavailable."""
    return utils_get_backend_version(backend)


def check_available(backend: str) -> bool:
    """Return True if required modules for ``backend`` can be imported."""
    return utils_check_available(backend)


def try_configure(backend: str) -> bool:
    """Attempt to configure unipandas for ``backend``.

    Returns False with a concise message if the backend is unavailable or
    initialization fails, allowing the caller to skip gracefully.
    """
    try:
        if not check_available(backend):
            print(f"[skip] backend={backend}: required modules not available")
            return False
        configure_backend(backend)
        return True
    except Exception as e:
        print(f"[skip] backend={backend}: {e}")
        return False


def _format_fixed_width_table(
    headers: List[str], rows: List[List[str]], right_align_from: int = 2
) -> List[str]:
    """Return a fixed-width table as list of strings for aligned printing."""
    widths = [
        max(len(headers[i]), max((len(r[i]) for r in rows), default=0)) for i in range(len(headers))
    ]

    def fmt_row(vals: List[str]) -> str:
        parts: List[str] = []
        for i, v in enumerate(vals):
            if i < right_align_from:
                parts.append(v.ljust(widths[i]))
            else:
                parts.append(v.rjust(widths[i]))
        return "  ".join(parts)

    lines: List[str] = [fmt_row(headers), "  ".join(("-" * w) for w in widths)]
    for row in rows:
        lines.append(fmt_row(row))
    return lines


def _used_cores_for_backend(backend: str) -> Optional[int]:
    try:
        return utils_used_cores_for_backend(backend)
    except Exception:
        return None


def _count_rows_backend(df) -> int:
    """Count rows without collecting entire dataset."""
    try:
        import pandas as _pd  # type: ignore
        if isinstance(df, _pd.DataFrame):
            return int(len(df.index))
    except Exception:
        pass
    try:
        import dask.dataframe as _dd  # type: ignore
        from dask.dataframe import DataFrame as _DaskDF  # type: ignore
        if isinstance(df, _DaskDF):
            return int(df.shape[0].compute())
    except Exception:
        pass
    try:
        import pyspark.pandas as _ps  # type: ignore
        from pyspark.pandas.frame import DataFrame as _PsDF  # type: ignore
        if isinstance(df, _PsDF):
            return int(df.to_spark().count())
    except Exception:
        pass
    try:
        import polars as _pl  # type: ignore
        if isinstance(df, _pl.DataFrame):
            return int(df.height)
    except Exception:
        pass
    try:
        return int(len(df))
    except Exception:
        return 0


def run_backends(path: str, query: Optional[str], assign: bool, groupby: Optional[str]) -> List["PerfResult"]:
    results: List["PerfResult"] = []
    for backend in Backends:
        if not try_configure(backend):
            continue
        t0 = time.perf_counter()
        df = read_csv(path)
        t1 = time.perf_counter()
        out = df
        if assign:
            try:
                out = out.assign(c=lambda x: x["a"] + x["b"])  # type: ignore
            except Exception:
                pass
        if query:
            try:
                out = out.query(query)
            except Exception:
                pass
        groups = None
        if groupby:
            try:
                out = out.groupby(groupby).agg({groupby: "count"})
                groups = _count_rows_backend(out.to_backend()) if hasattr(out, "to_backend") else None
            except Exception:
                groups = None
        t2 = time.perf_counter()
        _ = out.head(1000000000).to_pandas()
        t3 = time.perf_counter()
        input_rows = _count_rows_backend(df.to_backend()) if hasattr(df, "to_backend") else None
        used = _used_cores_for_backend(backend)
        ver = get_backend_version(backend)
        print(f"[info] backend={backend} version={ver} used_cores={used} available_cores={os.cpu_count()}")
        if PerfResult is None:
            # Minimal fallback object to keep code flowing in rare envs
            class _Tmp:  # type: ignore
                def __init__(self, **kw):
                    self.__dict__.update(kw)
            results.append(_Tmp(backend=backend, read_seconds=t1 - t0, compute_seconds=t3 - t2, input_rows=input_rows, groups=groups, used_cores=used, ok=True))
        else:
            results.append(
                PerfResult.now(
                    frontend="unipandas",
                    backend=backend,
                    operation="bench",
                    dataset_rows=None,
                    input_rows=input_rows,
                    read_seconds=t1 - t0,
                    compute_seconds=t3 - t2,
                    groups=groups,
                    used_cores=used,
                    ok=True,
                    notes=None,
                )
            )
    return results


def log_results(results: List["PerfResult"], jsonl_out: Optional[str]) -> None:
    if not jsonl_out:
        return
    path = Path(jsonl_out)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Prefer perfcore writer when available
    if PerfResult is not None and perf_write_results is not None:
        perf_write_results(path, results, append=False)
        return
    # Fallback: write minimal JSONL lines
    import json as _json
    with path.open("w") as f:
        for r in results:
            payload = {
                "frontend": getattr(r, "frontend", "unipandas"),
                "backend": getattr(r, "backend", None),
                "operation": getattr(r, "operation", "bench"),
                "input_rows": getattr(r, "input_rows", None),
                "read_seconds": getattr(r, "read_seconds", None),
                "compute_seconds": getattr(r, "compute_seconds", None),
                "groups": getattr(r, "groups", None),
                "used_cores": getattr(r, "used_cores", None),
                "ok": getattr(r, "ok", True),
            }
            f.write(_json.dumps(payload) + "\n")


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments (short and readable)."""
    p = argparse.ArgumentParser(description="Benchmark unipandas across backends")
    p.add_argument("path", help="Path to CSV file to read")
    p.add_argument("--query", default=None, help="Optional pandas query string, e.g. 'a > 0'")
    p.add_argument("--assign", action="store_true", help="Add column c = a + b before compute")
    p.add_argument("--groupby", default="cat", help="Groupby column name to count (default: cat)")
    p.add_argument("--code-file", default=None, help="Optional path to the pandas code file processed")
    p.add_argument("--md-out", default=None, help="If set, write results as Markdown to this file")
    p.add_argument("--jsonl-out", default=None, help="If set, write results JSONL via perfcore to this file")
    p.add_argument("--auto-rows", type=int, default=None, help="If set, generate a synthetic CSV with this many rows and use it")
    p.add_argument("--use-existing-1m", action="store_true", help="Use data/bench_1000000.csv if present")
    return p.parse_args()


def _availability() -> List[Dict[str, object]]:
    """Build backend availability list with versions."""
    info: List[Dict[str, object]] = []
    for name in Backends:
        info.append({"backend": name, "version": get_backend_version(name), "available": check_available(name)})
    return info


def _rows_for_console(results: List["PerfResult"], availability: List[Dict[str, object]]) -> List[List[str]]:
    """Build console/markdown rows in a consistent backend order."""
    by_backend = {r.backend: r for r in results}
    rows: List[List[str]] = []
    for name in Backends:
        r = by_backend.get(name)
        if r:
            used = getattr(r, "used_cores", None) or os.cpu_count()
            ver = get_backend_version(name)
            load_s = getattr(r, "read_seconds", None)
            comp_s = getattr(r, "compute_seconds", None)
            rows.append([
                name,
                str(ver),
                f"{load_s:.4f}" if isinstance(load_s, float) else "-",
                f"{comp_s:.4f}" if isinstance(comp_s, float) else "-",
                str(getattr(r, "input_rows", "-")),
                str(used),
            ])
        else:
            ver = next((a["version"] for a in availability if a["backend"] == name), None)
            rows.append([name, str(ver), "-", "-", "-", "-"])
    return rows


def render_markdown(results: List["PerfResult"], args: argparse.Namespace, md_out: Optional[str]) -> None:
    if not md_out:
        return
    availability = _availability()
    rows = _rows_for_console(results, availability)
    try:
        from mdreport import Report, system_info  # type: ignore
    except Exception:
        content: List[str] = ["# unipandas benchmark", "", "## Run context", ""]
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        content += [
            f"- Data file: `{args.path}`",
            f"- Ran at: {ts}",
            f"- Python: `{platform.python_version()}` on `{platform.platform()}`",
            f"- Args: assign={args.assign}, query={args.query!r}, groupby={args.groupby!r}",
            "",
            "## Backend availability",
            "",
            "| backend | version | status |",
            "|---|---|---|",
            *[f"| {it['backend']} | {it['version']} | {'available' if it['available'] else 'unavailable'} |" for it in availability],
            "",
            "## Results (seconds)",
            "",
            "```text",
            *_format_fixed_width_table(["backend", "version", "load_s", "compute_s", "input_rows", "used_cores"], rows),
            "```",
            "",
        ]
        with open(md_out, "w") as f:
            f.write("\n".join(content))
        print(f"\nWrote Markdown results to {md_out}")
    else:
        rpt = Report(md_out)
        rpt.title("unipandas benchmark").preface([
            "## Run context",
            f"- Data file: `{args.path}`",
            f"- Ran at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            *system_info(),
            f"- Args: assign={args.assign}, query={args.query!r}, groupby={args.groupby!r}",
            "",
            "## Backend availability",
            "",
            "| backend | version | status |",
            "|---|---|---|",
            *[f"| {it['backend']} | {it['version']} | {'available' if it['available'] else 'unavailable'} |" for it in availability],
            "",
            "## Results (seconds)",
            "",
        ])
        rpt.table(["backend", "version", "load_s", "compute_s", "input_rows", "used_cores"], rows, align_from=2, style="fixed").write()
        print(f"\nWrote Markdown results to {md_out}")


def main() -> int:
    args = _parse_args()
    # Optional data synthesis
    if args.use_existing_1m:
        candidate = Path(__file__).resolve().parents[2] / "data" / "bench_1000000.csv"
        if candidate.exists():
            args.path = str(candidate)
    elif args.auto_rows:
        import csv as _csv, random as _random
        data_dir = Path(__file__).resolve().parents[2] / "data"
        data_dir.mkdir(exist_ok=True)
        gen_path = data_dir / f"bench_{args.auto_rows}.csv"
        if not gen_path.exists():
            _random.seed(42)
            with gen_path.open("w", newline="") as f:
                w = _csv.writer(f)
                w.writerow(["a", "b", "cat"])  # header
                for _ in range(int(args.auto_rows)):
                    w.writerow([
                        _random.randint(-1000, 1000),
                        _random.randint(-1000, 1000),
                        _random.choice(["x", "y", "z"]),
                    ])
        args.path = str(gen_path)

    print("Backends:", Backends)
    results = run_backends(args.path, query=args.query, assign=args.assign, groupby=args.groupby)
    if not results:
        print("No backends produced results.")
        return 1
    # Console summary
    rows = _rows_for_console(results, _availability())
    for line in _format_fixed_width_table(["backend", "version", "load_s", "compute_s", "input_rows", "used_cores"], rows):
        print(line)
    # Persist and report
    log_results(results, args.jsonl_out)
    render_markdown(results, args, args.md_out)
    return 0


if __name__ == "__main__":
    sys.exit(main())


