from __future__ import annotations

from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

import sys
from pathlib import Path as _Path
sys.path.insert(0, str(_Path(__file__).resolve().parents[1] / "scripts" / "brc"))
from billion_row_challenge import _total_rows_from_parquet  # type: ignore


def test_parquet_footer_rowcount_multiple_files(tmp_path: Path):
    p1 = tmp_path / "p1.parquet"
    p2 = tmp_path / "p2.parquet"
    pq.write_table(pa.table({"a": list(range(11))}), p1)
    pq.write_table(pa.table({"a": list(range(9))}), p2)
    assert _total_rows_from_parquet([p1, p2]) == 20


