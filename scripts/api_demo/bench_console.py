from __future__ import annotations

"""Thin wrapper to import console summary printer for benches.

Kept for backwards-compatibility; prefer importing console.bench directly.
"""

from typing import List

from console.bench import print_bench_console_summary, HEADERS  # type: ignore


