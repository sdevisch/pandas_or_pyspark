#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Dict

from perfcore.result import Result


def _read_jsonl(path: Path) -> List[Result]:
    if not path.exists():
        return []
    return [Result.from_json(line) for line in path.read_text().splitlines() if line.strip()]


def _format_fixed(headers: List[str], rows: List[List[str]]) -> List[str]:
    widths = [max(len(headers[i]), max((len(r[i]) for r in rows), default=0)) for i in range(len(headers))]
    def fmt(vals: List[str]) -> str:
        parts: List[str] = []
        for i, v in enumerate(vals):
            parts.append(v.ljust(widths[i]) if i < 2 else v.rjust(widths[i]))
        return "  ".join(parts)
    lines = [fmt(headers), "  ".join(("-" * w) for w in widths)]
    for r in rows:
        lines.append(fmt(r))
    return lines


def main() -> int:
    p = argparse.ArgumentParser(description="Render performance matrix from JSONL results")
    p.add_argument("--in", dest="inp", default="results/perf.jsonl")
    p.add_argument("--out", dest="out", default="reports/perf/perf_matrix.md")
    args = p.parse_args()

    path = Path(args.inp)
    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)

    results = _read_jsonl(path)
    # Pivot by (frontend, backend) with compute_seconds
    frontends = sorted({r.frontend for r in results})
    backends = sorted({r.backend for r in results})
    latest: Dict[tuple, Result] = {}
    for r in results:
        latest[(r.frontend, r.backend)] = r
    headers = ["frontend/backend"] + backends
    rows: List[List[str]] = []
    for fe in frontends:
        row = [fe]
        for be in backends:
            r = latest.get((fe, be))
            row.append(f"{(r.compute_seconds or 0):.4f}" if r and r.ok and r.compute_seconds is not None else "-")
        rows.append(row)

    # Prefer mdreport if available
    try:
        from mdreport import Report  # type: ignore
    except Exception:
        content = ["# Performance matrix", "", "```text", *_format_fixed(headers, rows), "```", ""]
        outp.write_text("\n".join(content))
        print("Wrote", outp)
    else:
        rpt = Report(outp)
        rpt.title("Performance matrix").table(headers, rows, align_from=2, style="fixed").write()
        print("Wrote", outp)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


