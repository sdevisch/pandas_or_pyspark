from __future__ import annotations

from pathlib import Path
from typing import List, Tuple
import time

from unipandas import configure_backend

# Absolute imports with project root on path
import sys as _sys
from pathlib import Path as _Path

_ROOT = _Path(__file__).resolve().parents[2]
if str(_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_ROOT))

from scripts.brc.brc_shared import read_frames_for_backend, concat_frames, measure_read  # type: ignore


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
        from scripts.brc.brc_shared import _materialize_count  # type: ignore

        rows = _materialize_count(backend_obj)
    else:
        pdf_all = out.to_pandas()
        rows = len(pdf_all.index) if hasattr(pdf_all, "index") else 0
    t3 = time.perf_counter()
    return rows, t3 - t2


