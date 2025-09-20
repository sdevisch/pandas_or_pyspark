from __future__ import annotations

from typing import List, Dict, Any
from datetime import datetime
from pathlib import Path

try:
    from .report import Report, system_info  # type: ignore
except Exception:
    Report = None  # type: ignore
    system_info = lambda: []  # type: ignore


def render_bench_markdown(md_out: str, args: Any, availability: List[Dict[str, object]], headers: List[str], rows: List[List[str]]) -> None:
    if Report is None:
        # Fallback simple writer
        content: List[str] = ["# unipandas benchmark", "", "## Run context", ""]
        content += [
            f"- Data file: `{args.path}`",
            f"- Ran at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
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
        ]
        # naive fixed-width
        widths = [max(len(headers[i]), max((len(r[i]) for r in rows), default=0)) for i in range(len(headers))]
        def fmt(vals: List[str]) -> str:
            return "  ".join(v.ljust(widths[i]) if i == 0 else v.rjust(widths[i]) for i, v in enumerate(vals))
        content += [fmt(headers), "  ".join('-' * w for w in widths)]
        for row in rows:
            content.append(fmt(row))
        content += ["```", ""]
        Path(md_out).write_text("\n".join(content))
        return

    rpt = Report(md_out)
    rpt.title("unipandas benchmark").preface([
        "## Run context",
        f"- Data file: `{args.path}`",
        f"- Ran at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        *system_info(),
        "",
        "## Backend availability",
        "",
        "| backend | version | status |",
        "|---|---|---|",
        *[f"| {it['backend']} | {it['version']} | {'available' if it['available'] else 'unavailable'} |" for it in availability],
        "",
        "## Results (seconds)",
        "",
    ]).table(headers, rows, align_from=2, style="fixed").write()
