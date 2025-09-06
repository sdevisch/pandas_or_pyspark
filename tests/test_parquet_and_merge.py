import pandas as pd
from pathlib import Path

from unipandas import configure_backend
from unipandas.io import read_parquet


def test_read_parquet_and_merge(tmp_path: Path):
    configure_backend("pandas")
    p1 = tmp_path / "a.parquet"
    p2 = tmp_path / "b.parquet"
    df1 = pd.DataFrame({"k": [1, 2, 3], "x": [10, 20, 30]})
    df2 = pd.DataFrame({"k": [2, 3, 4], "y": [200, 300, 400]})
    df1.to_parquet(p1, index=False)
    df2.to_parquet(p2, index=False)

    f1 = read_parquet(str(p1))
    f2 = read_parquet(str(p2))
    merged = f1.merge(f2, on="k")
    pdf = merged.to_pandas()
    assert list(pdf.columns) == ["k", "x", "y"]
    assert set(pdf["k"]) == {2, 3}


