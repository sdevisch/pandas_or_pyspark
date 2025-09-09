from __future__ import annotations

from pathlib import Path
from typing import List, Tuple
import time

from unipandas import configure_backend
try:
    # Package-style import
    from .brc_shared import read_frames_for_backend, concat_frames, measure_read  # type: ignore
except Exception:
    # Script-style fallback
    import sys as _sys
    from pathlib import Path as _Path

    _here = _Path(__file__).resolve()
    _sys.path.append(str(_here.parents[0]))  # scripts/brc
    _sys.path.append(str(_here.parents[1]))  # scripts
    from brc_shared import read_frames_for_backend, concat_frames, measure_read  # type: ignore


def combine_chunks(chunks: List[Path], backend: str):
    frames = read_frames_for_backend(chunks, backend)
    return concat_frames(frames, backend)


def compute_op_and_count(combined, op: str, materialize: str) -> Tuple[int, float]:
    t2 = time.perf_counter()
    out = combined.query("x > 0 and y < 0") if op == "filter" else combined.groupby("cat").agg({"x": "sum", "y": "mean"})
    if materialize == "head":
        materialize = "count"
    if materialize == "count":
        backend_obj = out.to_backend()
        from scripts.brc.billion_row_challenge import _materialize_count  # type: ignore

        rows = _materialize_count(backend_obj)
    else:
        pdf_all = out.to_pandas()
        rows = len(pdf_all.index) if hasattr(pdf_all, "index") else 0
    t3 = time.perf_counter()
    return rows, t3 - t2


