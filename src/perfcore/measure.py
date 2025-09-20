from __future__ import annotations

from pathlib import Path
from typing import List
from time import perf_counter

from .result import Result
from unipandas import configure_backend
from unipandas.io import read_parquet


def _count_rows(obj) -> int:
    try:
        import pandas as _pd  # type: ignore
        if isinstance(obj, _pd.DataFrame):
            return int(len(obj.index))
    except Exception:
        pass
    try:
        import dask.dataframe as _dd  # type: ignore
        from dask.dataframe import DataFrame as _DaskDF  # type: ignore
        if isinstance(obj, _DaskDF):
            return int(obj.shape[0].compute())
    except Exception:
        pass
    try:
        import pyspark.pandas as _ps  # type: ignore
        from pyspark.pandas.frame import DataFrame as _PsDF  # type: ignore
        if isinstance(obj, _PsDF):
            return int(obj.to_spark().count())
    except Exception:
        pass
    try:
        import polars as _pl  # type: ignore
        if isinstance(obj, _pl.DataFrame):
            return int(obj.height)
    except Exception:
        pass
    try:
        return int(len(obj))
    except Exception:
        return 0


def measure_once(frontend: str, backend: str, dataset_glob: str, materialize: str = "count") -> Result:
    # For now, frontend is informational; we configure unipandas backend and run groupby-only
    r = Result.now(frontend=frontend, backend=backend, operation="groupby")
    try:
        import glob
        import pyarrow.parquet as _pq  # type: ignore
        chunk_paths = [Path(p) for p in glob.glob(dataset_glob)]
        r.dataset_rows = None
        r.input_rows = sum(int(_pq.ParquetFile(str(p)).metadata.num_rows) for p in chunk_paths) if chunk_paths else None
    except Exception:
        chunk_paths = []
    try:
        configure_backend(backend)
        t0 = perf_counter()
        frames = [read_parquet(str(p)) for p in chunk_paths]
        t1 = perf_counter()
        # Concat via pandas for simplicity; could route via scripts/brc helpers
        import pandas as pd  # type: ignore
        combined = pd.concat([f.to_backend() for f in frames]) if frames else pd.DataFrame()
        r.read_seconds = t1 - t0
        # Run groupby agg
        t2 = perf_counter()
        out = combined.groupby("cat").agg({"x": "sum", "y": "mean"}) if not combined.empty else combined
        if materialize == "head":
            materialize = "count"
        if materialize == "count":
            r.groups = _count_rows(out)
        else:
            _ = out.head(10_000)
            r.groups = _count_rows(out)
        t3 = perf_counter()
        r.compute_seconds = t3 - t2
        r.ok = True
    except Exception as e:
        r.ok = False
        r.notes = str(e)
    return r


def write_result(r: Result, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("a") as f:
        f.write(r.to_json() + "\n")


def write_results(results: List[Result], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("a") as f:
        for r in results:
            f.write(r.to_json() + "\n")


