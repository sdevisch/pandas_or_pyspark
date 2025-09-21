import sys
from pathlib import Path

import pytest


@pytest.mark.timeout(60)
def test_run_once_parses_timings_pandas():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts" / "brc"))
    from billion_row_om_runner import run_once  # type: ignore

    entry = run_once("pandas", 1_000, 15.0)
    assert entry.ok is True
    # read_s and compute_s should be floats or None
    assert entry.read_s is None or isinstance(entry.read_s, float)
    assert entry.compute_s is None or isinstance(entry.compute_s, float)


def test_count_input_rows_matches_parquet_metadata(tmp_path):
    # Use canonical helper from challenge
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts" / "brc"))
    from billion_row_challenge import _count_input_rows  # type: ignore

    import pyarrow as pa  # type: ignore
    import pyarrow.parquet as pq  # type: ignore

    # Create two Parquet parts with known row counts
    part_dir = tmp_path / "parts"
    part_dir.mkdir()

    tbl1 = pa.table({"a": list(range(5))})  # 5 rows
    tbl2 = pa.table({"a": list(range(7))})  # 7 rows
    p1 = part_dir / "part1.parquet"
    p2 = part_dir / "part2.parquet"
    pq.write_table(tbl1, p1)
    pq.write_table(tbl2, p2)

    # Count using metadata via the helper
    glob_path = str(part_dir / "*.parquet")
    metadata_count = _count_input_rows(0, glob_path)

    # Independently read actual rows to validate the metadata-based count
    actual_count = pq.read_table(p1).num_rows + pq.read_table(p2).num_rows

    assert metadata_count == actual_count == 12

