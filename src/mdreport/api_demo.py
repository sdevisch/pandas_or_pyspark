from __future__ import annotations

"""Markdown renderers for API demo reports.

These helpers centralize Markdown assembly outside of orchestration scripts.
"""

from datetime import datetime
from typing import List

from .report import Report, system_info


def render_compat_matrix(md_out: str, headers: List[str], rows: List[List[str]]) -> None:
    """Write the compatibility matrix to a Markdown file.

    - md_out: destination path
    - headers: table headers
    - rows: table rows
    """
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rpt = Report(md_out)
    rpt.title("Compatibility matrix").preface([
        f"Generated at: {ts}",
        *system_info(),
        "",
    ]).table(headers, rows, align_from=1, style="fixed").write()


def render_relational_demos(md_out: str, headers: List[str], rows: List[List[str]]) -> None:
    """Write the relational API demos report to a Markdown file.

    - md_out: destination path
    - headers: table headers
    - rows: table rows
    """
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rpt = Report(md_out)
    rpt.title("Relational API demos").preface([
        f"Generated at: {ts}",
        *system_info(),
        "",
    ]).table(headers, rows, align_from=3, style="fixed").write()


