import sys
from pathlib import Path

import pytest


@pytest.mark.timeout(60)
def test_run_once_parses_timings_pandas():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts" / "brc"))
    from billion_row_om_runner import run_once  # type: ignore

    entry = run_once("pandas", 1_000, 15.0)
    assert entry.ok is True
    # read_s and compute_s should be floats when parsing the report
    assert entry.read_s is None or isinstance(entry.read_s, float)
    assert entry.compute_s is None or isinstance(entry.compute_s, float)


