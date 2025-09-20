from __future__ import annotations

import os
from unipandas import configure_backend
from unipandas.io import read_parquet
from unipandas.frame import Frame


def load(glob_path: str) -> Frame:
    os.environ["UNIPANDAS_BACKEND"] = "pyspark"
    configure_backend("pyspark")
    import glob
    frames = [read_parquet(p) for p in glob.glob(glob_path)]
    import pyspark.pandas as ps  # type: ignore
    return Frame(ps.concat([f.to_backend() for f in frames])) if frames else Frame(ps.DataFrame())


def groupby_agg(df: Frame) -> Frame:
    return df.groupby("cat").agg({"x": "sum", "y": "mean"})


def count_rows(df: Frame) -> int:
    return int(df.to_backend().to_spark().count())


