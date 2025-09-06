import pytest
import pandas as pd
from pathlib import Path

from unipandas import configure_backend
from unipandas.io import read_csv


@pytest.mark.parametrize("backend,mod", [("polars", "polars"), ("duckdb", "duckdb")])
def test_io_backends_convert_to_pandas(tmp_path: Path, backend: str, mod: str):
    pytest.importorskip(mod)
    p = tmp_path / "sample.csv"
    pd.DataFrame({"a": [1, 2], "b": [3, 4]}).to_csv(p, index=False)

    configure_backend(backend)
    f = read_csv(str(p))
    pdf = f.to_pandas()
    assert list(pdf.columns) == ["a", "b"]
    assert len(pdf) == 2


