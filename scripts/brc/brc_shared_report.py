from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class SummaryRow:
    backend: str
    version: Optional[str]
    op: str
    read_s: float
    compute_s: float
    rows: Optional[int]
    used_cores: Optional[int]
    groups: Optional[int] = None


def build_summary_rows(results: List[SummaryRow], include_groups: bool) -> List[List[str]]:
    def fmt_num(v: Optional[float]) -> str:
        return f"{v:.4f}" if isinstance(v, float) and v and v > 0 else "-"

    out: List[List[str]] = []
    for r in results:
        row = [
            r.backend,
            str(r.version),
            r.op,
            fmt_num(r.read_s),
            fmt_num(r.compute_s),
            str(r.rows) if r.rows is not None else "-",
            str(r.used_cores) if r.used_cores else "-",
        ]
        if include_groups:
            row.append(str(r.groups) if r.groups is not None else "-")
        out.append(row)
    return out


