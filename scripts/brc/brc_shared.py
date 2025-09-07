from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Tuple
import time

from unipandas.io import read_csv, read_parquet
from unipandas.frame import Frame


def read_frames_for_backend(chunks: List[Path], backend: str) -> List[Frame]:
    """Read chunk files into backend dataframes and wrap as ``Frame``.

    The IO path supports both Parquet and CSV based on each chunk's file
    extension.
    """
    frames: List[Frame] = []
    for p in chunks:
        if p.suffix.lower() == ".parquet":
            frames.append(read_parquet(str(p)))
        else:
            frames.append(read_csv(str(p)))
    return frames


def concat_frames(frames: List[Frame], backend: str) -> Frame:
    """Concatenate a list of Frames using backend-native concat semantics."""
    if backend == "pyspark":
        import pyspark.pandas as ps  # type: ignore
        return Frame(ps.concat([f.to_backend() for f in frames]))
    if backend == "dask":
        import dask.dataframe as dd  # type: ignore
        return Frame(dd.concat([f.to_backend() for f in frames]))
    import pandas as pd  # type: ignore
    return Frame(pd.concat([f.to_backend() for f in frames]))


def measure_read(chunks: List[Path], backend: str) -> tuple[Frame, float, int]:
    """Measure time to read and concatenate all chunks for a backend.

    Returns the combined Frame, elapsed seconds, and total input bytes.
    """
    t0 = time.perf_counter()
    frames = read_frames_for_backend(chunks, backend)
    combined = concat_frames(frames, backend)
    t1 = time.perf_counter()
    total_bytes = sum((p.stat().st_size for p in chunks if p.exists()), 0)
    return combined, t1 - t0, total_bytes


def _materialize_count(backend_df) -> int:
    """Return row count for different backends without collecting all rows.

    This forces full evaluation while avoiding a full to_pandas transfer.
    """
    try:
        import pandas as _pd  # type: ignore
        if isinstance(backend_df, _pd.DataFrame):
            return int(len(backend_df.index))
    except Exception:
        pass
    try:
        import dask.dataframe as _dd  # type: ignore
        from dask.dataframe import DataFrame as _DaskDF  # type: ignore
        if isinstance(backend_df, _DaskDF):
            return int(backend_df.shape[0].compute())
    except Exception:
        pass
    try:
        import pyspark.pandas as _ps  # type: ignore
        from pyspark.pandas.frame import DataFrame as _PsDF  # type: ignore
        if isinstance(backend_df, _PsDF):
            sdf = backend_df.to_spark()
            return int(sdf.count())
    except Exception:
        pass
    try:
        import polars as _pl  # type: ignore
        if isinstance(backend_df, _pl.DataFrame):
            return int(backend_df.height)
    except Exception:
        pass
    try:
        import duckdb as _duck  # type: ignore
        if hasattr(backend_df, "to_df"):
            con = _duck.connect()
            try:
                rel = backend_df
                return int(con.execute("SELECT COUNT(*) FROM rel").fetchone()[0])
            finally:
                con.close()
    except Exception:
        pass
    # Fallback
    try:
        return int(len(backend_df))
    except Exception:
        return 0


def run_operation(combined: Frame, op: str, materialize: str) -> tuple[int, float]:
    """Execute the chosen operation and fully evaluate the result.

    Returns the observed row count and the compute duration.
    """
    t2 = time.perf_counter()
    out = combined.query("x > 0 and y < 0") if op == "filter" else combined.groupby("cat").agg({"x": "sum", "y": "mean"})
    if materialize == "head":
        materialize = "count"
    if materialize == "count":
        backend_obj = out.to_backend()
        rows = _materialize_count(backend_obj)
    else:  # materialize == "all"
        pdf_all = out.to_pandas()
        rows = len(pdf_all.index) if hasattr(pdf_all, "index") else 0
    t3 = time.perf_counter()
    return rows, t3 - t2


