from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REPORTS_BRC = ROOT / "reports" / "brc"
RESULTS = ROOT / "results"

# Canonical report files
REPORT_MAIN = REPORTS_BRC / "brc_1b_groupby.md"
REPORT_SMOKE = REPORTS_BRC / "brc_smoke_groupby.md"
REPORT_OM = REPORTS_BRC / "brc_order_of_magnitude.md"
REPORT_1MIN = REPORTS_BRC / "brc_under_1min_capacity.md"

# Canonical result files
JSONL_1B = RESULTS / "brc_1b_groupby.jsonl"
