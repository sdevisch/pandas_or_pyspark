import os
import sys
from pathlib import Path

import pytest


@pytest.mark.timeout(60)
def test_can_process_within_pandas(tmp_path: Path):
    # Import function directly
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts" / "brc"))
    from brc_one_minute_runner import can_process_within  # type: ignore

    ok, elapsed = can_process_within("pandas", 1_000, 15.0, "filter", None)
    assert ok, f"pandas should process 1000 rows within budget, elapsed={elapsed:.3f}s"


