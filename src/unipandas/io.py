from __future__ import annotations

from typing import Any, Optional

from .backend import (
    current_backend_name,
    import_dask_dataframe,
    import_pandas,
    import_pyspark_pandas,
)
from .frame import Frame


def read_csv(path: str, **kwargs: Any) -> Frame:
    backend = current_backend_name()
    if backend == "pandas":
        pd = import_pandas()
        return Frame(pd.read_csv(path, **kwargs))
    if backend == "dask":
        dd = import_dask_dataframe()
        return Frame(dd.read_csv(path, **kwargs))
    if backend == "pyspark":
        ps = import_pyspark_pandas()
        return Frame(ps.read_csv(path, **kwargs))
    raise RuntimeError(f"Unknown backend {backend}")


def read_parquet(path: str, **kwargs: Any) -> Frame:
    backend = current_backend_name()
    if backend == "pandas":
        pd = import_pandas()
        return Frame(pd.read_parquet(path, **kwargs))
    if backend == "dask":
        dd = import_dask_dataframe()
        return Frame(dd.read_parquet(path, **kwargs))
    if backend == "pyspark":
        ps = import_pyspark_pandas()
        return Frame(ps.read_parquet(path, **kwargs))
    raise RuntimeError(f"Unknown backend {backend}")


