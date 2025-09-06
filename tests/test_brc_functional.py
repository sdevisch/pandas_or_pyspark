import sys
from pathlib import Path

import pandas as pd
import pytest


def _add_brc_to_path():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts" / "brc"))


@pytest.mark.timeout(60)
def test_can_process_within_groupby_pandas(tmp_path: Path):
    _add_brc_to_path()
    from brc_one_minute_runner import can_process_within  # type: ignore

    ok, elapsed = can_process_within("pandas", 2_000, 20.0, "groupby", None)
    assert ok


@pytest.mark.timeout(60)
def test_can_process_within_with_parquet_data_glob_pandas(tmp_path: Path):
    _add_brc_to_path()
    from brc_one_minute_runner import can_process_within  # type: ignore

    # Create tiny parquet dataset compatible with challenge schema
    out_dir = tmp_path / "parq"
    out_dir.mkdir()
    df = pd.DataFrame({"id": [0, 1, 2], "x": [1, -1, 2], "y": [0, -2, 5], "cat": ["x", "y", "x"]})
    p = out_dir / "chunk.parquet"
    df.to_parquet(p, index=False)

    ok, elapsed = can_process_within("pandas", 0, 15.0, "filter", str(out_dir / "*.parquet"))
    assert ok


