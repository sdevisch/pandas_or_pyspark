import pandas as pd

from unipandas import configure_backend
from unipandas.frame import Frame


def _make():
    pdf = pd.DataFrame({"a": [1, -1, 2], "b": [10, 20, -5], "g": ["x", "y", "x"]})
    return Frame(pdf)


def test_select_query_assign_merge_groupby():
    configure_backend("pandas")
    f = _make()
    out = f.select(["a", "b"]).query("a > 0").assign(c=lambda x: x["a"] + x["b"])  # type: ignore
    pdf = out.to_pandas()
    assert list(pdf.columns) == ["a", "b", "c"]
    assert (pdf["a"] > 0).all()

    # merge
    right = Frame(pd.DataFrame({"a": [1, 2], "z": [100, 200]}))
    merged = out.merge(right, on="a")
    mpdf = merged.to_pandas()
    assert "z" in mpdf.columns

    # groupby
    g = f.groupby("g").agg({"a": "sum"})
    gpdf = g.to_pandas()
    assert set(gpdf.index) == {"x", "y"}


