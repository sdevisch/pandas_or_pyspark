from __future__ import annotations

from typing import List

try:
    from console.bench import print_bench_console_summary, HEADERS  # type: ignore
except Exception:
    # Fallback no-op to avoid coupling; this module will be removed after migration
    HEADERS: List[str] = ["backend", "version", "load_s", "compute_s", "input_rows", "used_cores"]
    def print_bench_console_summary(rows: List[List[str]]) -> None:
        print(" ".join(HEADERS))
        for row in rows:
            print(" ".join(str(v) for v in row))


