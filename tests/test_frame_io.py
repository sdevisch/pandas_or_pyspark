import os
import pandas as pd

from unipandas import configure_backend
from unipandas.io import read_csv


def test_read_csv_and_basic_ops(tmp_path):
    configure_backend("pandas")
    p = tmp_path / "sample.csv"
    df = pd.DataFrame({"a": [1, -1, 2], "b": [10, 20, -5]})
    df.to_csv(p, index=False)

    f = read_csv(str(p))
    assert f.backend == "pandas"

    out = (
        f.select(["a", "b"])  # projection
         .query("a > 0")        # filter
         .assign(c=lambda x: x["a"] + x["b"])  # new column
    )

    pdf = out.head(10).to_pandas()
    assert list(pdf.columns) == ["a", "b", "c"]
    # rows where a>0: expect 1 and 2
    assert (pdf["a"] > 0).all()
