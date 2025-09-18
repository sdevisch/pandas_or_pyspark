from __future__ import annotations

from typing import Any, Optional

from .backend import (
    current_backend_name,
    import_dask_dataframe,
    import_pandas,
    import_pyspark_pandas,
    import_polars,
    import_duckdb,
    import_numpy,
    import_numba,
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
    if backend == "polars":
        # Use polars for IO, but convert to pandas for unified pandas interface
        pl = import_polars()
        pd = import_pandas()
        return Frame(pl.read_csv(path, **kwargs).to_pandas())
    if backend == "duckdb":
        duck = import_duckdb()
        # duckdb returns a relation; materialize to pandas for uniform ops
        con = duck.connect()
        try:
            rel = con.read_csv(path, **kwargs)
            return Frame(rel.to_df())
        finally:
            con.close()
    if backend == "numpy":
        # Simple CSV via pandas then expose underlying numpy array to Frame
        pd = import_pandas()
        return Frame(pd.read_csv(path, **kwargs).values)
    if backend == "numba":
        # Storage remains pandas; computation may be accelerated at op level
        pd = import_pandas()
        return Frame(pd.read_csv(path, **kwargs))
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
    if backend == "polars":
        pl = import_polars()
        pd = import_pandas()
        return Frame(pl.read_parquet(path, **kwargs).to_pandas())
    if backend == "duckdb":
        duck = import_duckdb()
        con = duck.connect()
        try:
            rel = con.read_parquet(path)
            return Frame(rel.to_df())
        finally:
            con.close()
    if backend == "numpy":
        # Read via pandas then expose ndarray
        pd = import_pandas()
        return Frame(pd.read_parquet(path, **kwargs).values)
    if backend == "numba":
        pd = import_pandas()
        return Frame(pd.read_parquet(path, **kwargs))
    raise RuntimeError(f"Unknown backend {backend}")


