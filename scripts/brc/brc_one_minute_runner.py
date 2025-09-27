#!/usr/bin/env python3

from __future__ import annotations

"""Compatibility shim for tests: exposes can_process_within.

Delegates to scripts.brc.one_minute_lib.can_process_within.
"""

from typing import Tuple, Optional
import argparse
import sys

from one_minute_lib import can_process_within  # type: ignore

__all__ = ["can_process_within"]


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Compatibility shim: one-minute runner")
    p.add_argument("--budget", type=float, default=60.0)
    p.add_argument("--data-glob", default=None)
    return p.parse_args()


def main() -> int:
    # Compatibility: accept CLI but do no heavy work; exit successfully.
    _ = _parse_args()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


