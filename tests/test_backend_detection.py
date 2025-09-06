import os
import importlib


def test_configure_backend_names():
    from unipandas.backend import configure_backend, current_backend_name

    for name in ["pandas", "dask", "pyspark", "polars", "duckdb"]:
        try:
            configure_backend(name)
            assert current_backend_name() == name
        except Exception:
            # not installed is fine for dask/pyspark/polars/duckdb
            if name == "pandas":
                raise


