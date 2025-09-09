from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional
from datetime import datetime
import os


def system_info() -> List[str]:
    import platform

    lines: List[str] = []
    lines.append(f"- Python: `{platform.python_version()}` on `{platform.platform()}`")
    lines.append(f"- CPU cores: {os.cpu_count()}")
    try:
        import psutil  # type: ignore

        mem_gb = getattr(psutil.virtual_memory(), "total", 0) / (1024 ** 3)
        lines.append(f"- RAM: {mem_gb:.1f} GiB")
    except Exception:
        pass
    return lines


def strip_h1_from_fragment(text: str) -> str:
    lines = text.splitlines()
    if lines and lines[0].lstrip().startswith("# "):
        return "\n".join(lines[1:]).lstrip("\n")
    return text


def resolve_report_path(path: os.PathLike | str) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _format_fixed(headers: List[str], rows: List[List[str]], right_align_from: int = 2) -> List[str]:
    widths = [max(len(headers[i]), max((len(r[i]) for r in rows), default=0)) for i in range(len(headers))]

    def fmt_row(vals: List[str]) -> str:
        parts: List[str] = []
        for i, v in enumerate(vals):
            if i < right_align_from:
                parts.append(v.ljust(widths[i]))
            else:
                parts.append(v.rjust(widths[i]))
        return "  ".join(parts)

    lines = [fmt_row(headers), "  ".join(("-" * w) for w in widths)]
    for r in rows:
        lines.append(fmt_row(r))
    return lines


def _format_pipe(headers: List[str], rows: List[List[str]]) -> List[str]:
    try:
        from tabulate import tabulate  # type: ignore

        return tabulate(rows, headers=headers, tablefmt="github").splitlines()
    except Exception:
        # Fallback to fixed if tabulate is unavailable
        return _format_fixed(headers, rows)


class Report:
    def __init__(self, out_path: os.PathLike | str):
        self.out_path = resolve_report_path(out_path)
        self._title: Optional[str] = None
        self._preface: List[str] = []
        self._headers: Optional[List[str]] = None
        self._rows: Optional[List[List[str]]] = None
        self._align_from: int = 2
        self._style: str = "fixed"
        self._suffix: List[str] = []

    def title(self, text: str) -> "Report":
        self._title = text
        return self

    def preface(self, lines: Iterable[str]) -> "Report":
        self._preface.extend(list(lines))
        return self

    def table(self, headers: List[str], rows: List[List[str]], *, align_from: int = 2, style: str = "fixed") -> "Report":
        self._headers = headers
        self._rows = rows
        self._align_from = align_from
        self._style = style
        return self

    def suffix(self, lines: Iterable[str]) -> "Report":
        self._suffix.extend(list(lines))
        return self

    def write(self, *, append: bool = False) -> Path:
        lines: List[str] = []
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        title = self._title or "Report"
        if append and self.out_path.exists():
            existing = self.out_path.read_text()
            self.out_path.write_text(existing + "\n\n")
        # Header and preface
        header_block: List[str] = [f"# {title}", "", f"Generated at: {ts}", ""]
        if self._preface:
            header_block.extend(list(self._preface))
        # Table
        table_lines: List[str] = []
        if self._headers is not None and self._rows is not None:
            if self._style == "pipe":
                table_lines = ["```text", *_format_pipe(self._headers, self._rows), "```", ""]
            else:
                table_lines = [
                    "```text",
                    *_format_fixed(self._headers, self._rows, right_align_from=self._align_from),
                    "```",
                    "",
                ]
        # Suffix
        suffix_block = list(self._suffix) if self._suffix else []
        # Assemble
        all_lines = [*header_block, *table_lines, *suffix_block]
        self.out_path.write_text("\n".join(all_lines))
        return self.out_path


