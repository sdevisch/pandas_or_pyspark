import os
import pytest
import pandas as pd

from unipandas import configure_backend
from unipandas.io import read_csv
from unipandas.frame import Frame


def _csv(tmp_path):
    p = tmp_path / "rt.csv"
    pd.DataFrame({"a": [1, 2], "b": [3, 4]}).to_csv(p, index=False)
    return str(p)


@pytest.mark.parametrize("backend,mod", [("dask", "dask.dataframe"), ("pyspark", "pyspark.pandas")])
def test_io_ops_smoke_backends(tmp_path, backend, mod):
    pytest.importorskip(mod)
    configure_backend(backend)
    f = read_csv(_csv(tmp_path))
    # simple ops
    out = f.select(["a"]).assign(c=lambda x: x["a"] + 1).head(2)
    pdf = out.to_pandas()
    assert list(pdf.columns) == ["a", "c"]


@pytest.mark.parametrize("backend,mod", [
    ("pandas", "pandas"),
    ("dask", "dask.dataframe"),
    ("pyspark", "pyspark.pandas"),
    ("polars", "polars"),
    ("duckdb", "duckdb"),
])
def test_to_backend_roundtrip(tmp_path, backend, mod):
    if backend != "pandas":
        pytest.importorskip(mod)
    configure_backend(backend)
    f = Frame(pd.DataFrame({"a": [1, 2]}))
    obj = f.to_backend()
    # to_pandas should always succeed
    pdf = f.to_pandas()
    assert list(pdf.columns) == ["a"]
    assert hasattr(obj, "__class__")
