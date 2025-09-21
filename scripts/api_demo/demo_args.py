from __future__ import annotations

"""Argument parser for the 3-minute API demo orchestrator.

This module is imported by the orchestration entrypoint and should not
perform any work at import time beyond defining the parser.
"""

import argparse


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the demo flow.

    Returns a simple namespace with:
    - budget_s: float (overall time budget in seconds; default 180)
    - out: optional explicit output Markdown path
    """
    p = argparse.ArgumentParser(description="Run the 3-minute unipandas API demo")
    p.add_argument(
        "--budget",
        dest="budget_s",
        type=float,
        default=180.0,
        help="Overall time budget in seconds (default: 180)",
    )
    p.add_argument(
        "--out",
        dest="out",
        default=None,
        help="Optional output Markdown path for the aggregated demo report",
    )
    return p.parse_args()


