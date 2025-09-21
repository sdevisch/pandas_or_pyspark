from __future__ import annotations

"""Helpers to create tiny demo datasets for the API demo flows.

No side effects at import. Functions are small and testable.
"""

from pathlib import Path
from typing import List


def ensure_smoke_csv(root: Path) -> Path:
    """Ensure a tiny CSV with columns a,b,cat exists under root/data.

    Returns the path to the CSV.
    """
    data = root / "data"
    data.mkdir(exist_ok=True)
    sample = data / "smoke.csv"
    if sample.exists():
        return sample
    import csv

    with sample.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["a", "b", "cat"])
        w.writerow([1, 2, "x"])
        w.writerow([-1, 5, "y"])
        w.writerow([3, -2, "x"])
    return sample


def ensure_tiny_parquet_glob(root: Path, rows: int = 1000) -> str:
    """Ensure a small parquet exists under root/data/brc_tmp_{rows} and return its glob."""
    d = root / "data" / f"brc_tmp_{rows}"
    p = d / "generated.parquet"
    if not p.exists():
        import pandas as _pd  # type: ignore
        import random as _rnd

        d.mkdir(parents=True, exist_ok=True)
        rng = _rnd.Random(42)
        pdf = _pd.DataFrame(
            {
                "id": list(range(rows)),
                "x": [rng.randint(-1000, 1000) for _ in range(rows)],
                "y": [rng.randint(-1000, 1000) for _ in range(rows)],
                "cat": [rng.choice(["x", "y", "z"]) for _ in range(rows)],
            }
        )
        pdf.to_parquet(str(p), index=False)
    return str(d / "*.parquet")
