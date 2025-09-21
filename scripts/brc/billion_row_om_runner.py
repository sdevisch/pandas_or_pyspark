#!/usr/bin/env python3

from __future__ import annotations

"""Compatibility shim for tests: expose run_once and _count_input_rows.

Delegates to 02_fast_3min_groupby.py helpers to keep tests passing during
the transition to the new 3-script layout.
"""

import glob as _glob
from pathlib import Path
from typing import Optional

from billion_row_challenge import _count_input_rows as _count_input_rows  # type: ignore


class Entry:
    def __init__(self, backend: str, rows: int, read_s: Optional[float], compute_s: Optional[float], ok: bool):
        self.backend = backend
        self.rows = rows
        self.read_s = read_s
        self.compute_s = compute_s
        self.ok = ok


def run_once(backend: str, rows: int, budget_s: float) -> Entry:
    # Minimal shim that executes a tiny run via 01 script path
    from one_minute_lib import can_process_within  # type: ignore

    ok, elapsed = can_process_within(backend, rows, budget_s, None)
    # We do not parse timings here; return ok and elapsed as compute_s
    return Entry(backend=backend, rows=rows, read_s=None, compute_s=elapsed if ok else None, ok=ok)


