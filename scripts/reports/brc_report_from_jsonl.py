#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

from perfcore.result import Result


def _read_jsonl(path: Path) -> List[Result]:
    if not path.exists():
        return []
    return [Result.from_json(line) for line in path.read_text().splitlines() if line.strip()]


def main() -> int:
    p = argparse.ArgumentParser(description="Render BRC report from JSONL results")
    p.add_argument("--in", dest="inp", required=True)
    p.add_argument("--out", dest="out", default="reports/brc/billion_row_challenge.md")
    args = p.parse_args()

    path = Path(args.inp)
    outp = Path(args.out)

    results = _read_jsonl(path)
    # If writing to default main report, auto-route small runs to smoke report
    DEFAULT_MAIN = Path("reports/brc/billion_row_challenge.md")
    DEFAULT_SMOKE = Path("reports/brc/billion_row_challenge_smoke.md")
    if outp == DEFAULT_MAIN:
        try:
            max_input = max(int(r.input_rows or 0) for r in results) if results else 0
        except Exception:
            max_input = 0
        if max_input < 1_000_000_000:
            outp = DEFAULT_SMOKE
    outp.parent.mkdir(parents=True, exist_ok=True)
    # Filter to operation=groupby and latest per backend
    by_backend = {}
    for r in results:
        if r.operation != "groupby":
            continue
        by_backend[r.backend] = r
    backends = sorted(by_backend.keys())
    headers = ["backend", "compute_s", "groups"]
    rows: List[List[str]] = []
    for b in backends:
        r = by_backend[b]
        rows.append([b, f"{(r.compute_seconds or 0):.4f}", str(r.groups or "-")])

    try:
        from mdreport import Report  # type: ignore
    except Exception:
        width = [max(len(h), max((len(r[i]) for r in rows), default=0)) for i, h in enumerate(headers)]
        def fmt(vals: List[str]) -> str:
            return "  ".join(v.ljust(width[i]) if i == 0 else v.rjust(width[i]) for i, v in enumerate(vals))
        lines = ["# Billion Row Challenge (from JSONL)", "", "```text", fmt(headers), "  ".join("-" * w for w in width)]
        lines.extend(fmt(r) for r in rows)
        lines.extend(["```", ""])
        outp.write_text("\n".join(lines))
        print("Wrote", outp)
    else:
        rpt = Report(outp)
        rpt.title("Billion Row Challenge (from JSONL)").table(headers, rows, align_from=1, style="fixed").write()
        print("Wrote", outp)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
