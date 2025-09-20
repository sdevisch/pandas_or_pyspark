from __future__ import annotations

from unipandas.io import read_parquet
from unipandas.frame import Frame


def load(glob_path: str) -> Frame:
    import glob
    frames = [read_parquet(p) for p in glob.glob(glob_path)]
    # Concatenate via pandas
    import pandas as pd  # type: ignore
    return Frame(pd.concat([f.to_backend() for f in frames])) if frames else Frame(pd.DataFrame())


def groupby_agg(df: Frame) -> Frame:
    return df.groupby("cat").agg({"x": "sum", "y": "mean"})


def count_rows(df: Frame) -> int:
    return len(df.to_pandas().index)
