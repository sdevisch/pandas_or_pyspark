import sys
from pathlib import Path


def test_utils_format_fixed_basic():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    from utils import format_fixed  # type: ignore

    headers = ["h1", "h2", "h3"]
    rows = [["a", "1", "2"], ["bb", "10", "3"]]
    lines = format_fixed(headers, rows, right_align_from=1)
    assert isinstance(lines, list)
    assert len(lines) >= 3
    assert lines[0].strip().startswith("h1")


