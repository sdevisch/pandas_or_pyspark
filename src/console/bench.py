from __future__ import annotations

from typing import List


HEADERS: List[str] = ["backend", "version", "load_s", "compute_s", "input_rows", "used_cores"]


def print_bench_console_summary(rows: List[List[str]]) -> None:
    print(" ".join(HEADERS))
    for row in rows:
        print(" ".join(str(v) for v in row))


