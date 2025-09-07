"""Backend detection and configuration for a unified pandas-like API.

This module centralizes which computational backend is used and provides
helpers to import the appropriate libraries on demand.

Supported backends:
- "pandas": CPython, single-machine pandas
- "dask": Dask DataFrame for parallel/distributed
- "pyspark": pandas API on Spark (pyspark.pandas)
- "polars": Polars DataFrame
- "duckdb": DuckDB (reads into pandas for unified ops)
"""

from __future__ import annotations

import importlib
import os
from dataclasses import dataclass
from typing import Optional


BACKEND_ENV_VAR = "UNIPANDAS_BACKEND"


@dataclass(frozen=True)
class Backend:
    """Represents a chosen backend by name.

    name values: "pandas", "dask", or "pyspark".
    """

    name: str


_CURRENT_BACKEND: Optional[Backend] = None


def _import_optional(module_name: str):
    try:
        return importlib.import_module(module_name)
    except Exception:
        return None


def _detect_backend_by_environment() -> Optional[str]:
    value = os.getenv(BACKEND_ENV_VAR)
    if not value:
        return None
    normalized = value.strip().lower()
    if normalized in {"pandas", "pd"}:
        return "pandas"
    if normalized in {"dask", "dd"}:
        return "dask"
    if normalized in {"pyspark", "spark", "ps", "pandas_on_spark"}:
        return "pyspark"
    if normalized in {"polars", "pl"}:
        return "polars"
    if normalized in {"duckdb", "duck"}:
        return "duckdb"
    return None


def _is_running_inside_spark() -> bool:
    pyspark = _import_optional("pyspark")
    if pyspark is None:
        return False
    # Spark 3.4+: SparkSession.getActiveSession; older: getOrCreate would start a session, so avoid it.
    try:
        from pyspark.sql import SparkSession  # type: ignore

        active = SparkSession.getActiveSession()
        return active is not None
    except Exception:
        return False


def _env_pref_backend() -> Optional[str]:
    value = _detect_backend_by_environment()
    return value


def _spark_active_pref() -> Optional[str]:
    if _is_running_inside_spark() and _import_optional("pyspark.pandas") is not None:
        return "pyspark"
    return None


def _dask_context_pref() -> Optional[str]:
    if _import_optional("dask.dataframe") is not None and (
        os.getenv("DASK_SCHEDULER_ADDRESS")
        or os.getenv("DASK_DISTRIBUTED__SCHEDULER")
        or os.getenv("DASK_CONFIG")
    ):
        return "dask"
    return None


def _single_available_pref() -> Optional[str]:
    if _import_optional("pyspark.pandas") is not None:
        return "pyspark"
    if _import_optional("dask.dataframe") is not None:
        return "dask"
    if _import_optional("polars") is not None:
        return "polars"
    if _import_optional("duckdb") is not None:
        return "duckdb"
    return None


def _detect_backend_heuristic() -> str:
    # 1) Respect explicit environment configuration
    env_backend = _env_pref_backend()
    if env_backend:
        return env_backend

    # 2) Prefer Spark if active
    spark_pref = _spark_active_pref()
    if spark_pref:
        return spark_pref

    # 3) Prefer Dask when context variables indicate a Dask environment
    dask_pref = _dask_context_pref()
    if dask_pref:
        return dask_pref

    # 4) If one scalable engine is available, pick it
    single_pref = _single_available_pref()
    if single_pref:
        return single_pref

    # 5) Fallback to pandas
    return "pandas"


def get_backend() -> Backend:
    global _CURRENT_BACKEND
    if _CURRENT_BACKEND is None:
        _CURRENT_BACKEND = Backend(_detect_backend_heuristic())
    return _CURRENT_BACKEND


def configure_backend(name: str) -> Backend:
    """Force the backend by name: "pandas", "dask", or "pyspark".

    Returns the updated backend instance.
    """
    normalized = name.strip().lower()
    if normalized in {"pandas", "pd"}:
        value = "pandas"
    elif normalized in {"dask", "dd"}:
        value = "dask"
    elif normalized in {"pyspark", "spark", "ps", "pandas_on_spark"}:
        value = "pyspark"
    elif normalized in {"polars", "pl"}:
        value = "polars"
    elif normalized in {"duckdb", "duck"}:
        value = "duckdb"
    else:
        raise ValueError(
            f"Unknown backend '{name}'. Valid options are 'pandas', 'dask', 'pyspark'."
        )

    global _CURRENT_BACKEND
    _CURRENT_BACKEND = Backend(value)
    return _CURRENT_BACKEND


def current_backend_name() -> str:
    return get_backend().name


def import_pandas():
    mod = _import_optional("pandas")
    if mod is None:
        raise RuntimeError("pandas is not installed. Please install 'pandas'.")
    return mod


def import_dask_dataframe():
    mod = _import_optional("dask.dataframe")
    if mod is None:
        raise RuntimeError("Dask DataFrame is not installed. Please install 'dask[dataframe]'.")
    return mod


def import_pyspark_pandas():
    mod = _import_optional("pyspark.pandas")
    if mod is None:
        raise RuntimeError(
            "pandas API on Spark is not installed. Please install 'pyspark' (Spark 3.2+)."
        )
    return mod


def import_polars():
    mod = _import_optional("polars")
    if mod is None:
        raise RuntimeError("polars is not installed. Please install 'polars'.")
    return mod


def import_duckdb():
    mod = _import_optional("duckdb")
    if mod is None:
        raise RuntimeError("duckdb is not installed. Please install 'duckdb'.")
    return mod


def is_pandas() -> bool:
    return current_backend_name() == "pandas"


def is_dask() -> bool:
    return current_backend_name() == "dask"


def is_pyspark() -> bool:
    return current_backend_name() == "pyspark"


def is_polars() -> bool:
    return current_backend_name() == "polars"


def is_duckdb() -> bool:
    return current_backend_name() == "duckdb"


def infer_backend_from_object(obj) -> Optional[str]:
    try:
        import pandas as pd  # type: ignore

        if isinstance(obj, pd.DataFrame):
            return "pandas"
    except Exception:
        pass

    try:
        import dask.dataframe as dd  # type: ignore

        from dask.dataframe import DataFrame as DaskDF  # type: ignore

        if isinstance(obj, DaskDF):
            return "dask"
    except Exception:
        pass

    try:
        import pyspark.pandas as ps  # type: ignore

        from pyspark.pandas.frame import DataFrame as PsDF  # type: ignore

        if isinstance(obj, PsDF):
            return "pyspark"
    except Exception:
        pass

    try:
        import polars as pl  # type: ignore

        if isinstance(obj, pl.DataFrame):
            return "polars"
    except Exception:
        pass

    return None


