from __future__ import annotations

from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import re

import sys
from pathlib import Path as _Path

# Ensure scripts on sys.path for direct import in tests
sys.path.insert(0, str(_Path(__file__).resolve().parents[1] / "scripts" / "brc"))
from billion_row_challenge import Result, write_report  # type: ignore
from billion_row_challenge import _total_rows_from_parquet  # type: ignore


def _tmp(md: str, tmp_path: Path) -> Path:
    p = tmp_path / "out.md"
    p.write_text(md)
    return p


def test_write_report_includes_groups_for_groupby(tmp_path: Path):
    chunks = [tmp_path / "part-1.parquet"]
    # Create dummy file to satisfy total bytes calc
    chunks[0].write_bytes(b"\x00")
    results = [
        Result(backend="pandas", version="x", op="groupby", read_s=0.1, compute_s=0.2, rows=1000, used_cores=1, groups=3)
    ]
    out = tmp_path / "report.md"
    write_report(chunks, results, str(out), append=False, title_suffix="test")
    txt = out.read_text()
    # Header should contain 'groups' column
    assert re.search(r"^backend\s+version\s+op\s+read_s\s+compute_s\s+rows\s+used_cores\s+groups$", txt, re.M), txt
    # Rows should include input rows and groups
    assert re.search(r"^pandas\s+x\s+groupby\s+\d+\.\d{4}\s+\d+\.\d{4}\s+1000\s+1\s+3$", txt, re.M), txt


def test_write_report_excludes_groups_for_filter(tmp_path: Path):
    chunks = [tmp_path / "part-1.parquet"]
    chunks[0].write_bytes(b"\x00")
    results = [
        Result(backend="pandas", version="x", op="filter", read_s=0.1, compute_s=0.2, rows=42, used_cores=1)
    ]
    out = tmp_path / "report.md"
    write_report(chunks, results, str(out), append=False, title_suffix="test")
    txt = out.read_text()
    # No groups header for filter
    assert re.search(r"^backend\s+version\s+op\s+read_s\s+compute_s\s+rows\s+used_cores$", txt, re.M), txt
    assert re.search(r"^pandas\s+x\s+filter\s+\d+\.\d{4}\s+\d+\.\d{4}\s+42\s+1$", txt, re.M), txt


def test_total_rows_from_parquet_reads_footer(tmp_path: Path):
    # Create two tiny parquet files with known row counts
    def _make_parquet(p: Path, n: int) -> None:
        table = pa.table({"a": list(range(n))})
        pq.write_table(table, p)

    p1 = tmp_path / "part-00001.parquet"
    p2 = tmp_path / "part-00002.parquet"
    _make_parquet(p1, 7)
    _make_parquet(p2, 5)
    total = _total_rows_from_parquet([p1, p2])
    assert total == 12


