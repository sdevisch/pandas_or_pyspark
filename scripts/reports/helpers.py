from __future__ import annotations

from datetime import datetime
from typing import List, Dict


def fixed_table(headers: List[str], rows: List[List[str]], right_align_from: int = 1) -> List[str]:
    widths = [max(len(headers[i]), max((len(r[i]) for r in rows), default=0)) for i in range(len(headers))]

    def fmt(vals: List[str]) -> str:
        parts: List[str] = []
        for i, v in enumerate(vals):
            parts.append(v.ljust(widths[i]) if i < right_align_from else v.rjust(widths[i]))
        return "  ".join(parts)

    lines: List[str] = [fmt(headers), "  ".join(('-' * w) for w in widths)]
    for row in rows:
        lines.append(fmt(row))
    return lines


def render_bench_markdown(md_out: str, args, availability: List[Dict[str, object]], headers: List[str], rows: List[List[str]]) -> None:
    try:
        from mdreport import Report, system_info  # type: ignore
    except Exception:
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
            *fixed_table(headers, rows, right_align_from=2),
            "```",
            "",
        ]
        with open(md_out, "w") as f:
            f.write("\n".join(content))
    else:
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
        ])
        rpt.table(headers, rows, align_from=2, style="fixed").write()
