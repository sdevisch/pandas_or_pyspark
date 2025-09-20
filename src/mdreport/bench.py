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


def render_bench_from_results(results: List[Any], args: Any, md_out: str) -> None:
    """High-level renderer that derives availability/rows and writes Markdown.

    This keeps benchmark scripts free of markdown-specific logic.
    """
    # Try to import helpers to compute availability and rows
    try:
        from scripts.api_demo.bench_lib import build_availability, rows_for_console  # type: ignore
    except Exception:
        try:
            from api_demo.bench_lib import build_availability, rows_for_console  # type: ignore
        except Exception:
            build_availability = None  # type: ignore
            rows_for_console = None  # type: ignore

    availability: List[Dict[str, object]]
    rows: List[List[str]]
    headers = ["backend", "version", "load_s", "compute_s", "input_rows", "used_cores"]

    if build_availability and rows_for_console:
        availability = build_availability()
        rows = rows_for_console(results, availability)
    else:
        # Fallback minimal availability/rows
        availability = []
        rows = []
        for r in results:
            rows.append([
                getattr(r, "backend", "-"),
                "-",
                f"{getattr(r, 'read_seconds', 0.0):.4f}" if isinstance(getattr(r, "read_seconds", None), float) else "-",
                f"{getattr(r, 'compute_seconds', 0.0):.4f}" if isinstance(getattr(r, "compute_seconds", None), float) else "-",
                str(getattr(r, "input_rows", "-")),
                str(getattr(r, "used_cores", "-")),
            ])

    render_bench_markdown(md_out, args, availability, headers, rows)
