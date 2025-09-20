from __future__ import annotations

# Narwhals adapter: try to use narwhals, fallback to pandas DataFrame compatibility.
from unipandas.io import read_parquet
from unipandas.frame import Frame


def load(glob_path: str) -> Frame:
    import glob
    frames = [read_parquet(p) for p in glob.glob(glob_path)]
    try:
        import narwhals as nw  # type: ignore
        # Convert to pandas then to narwhals DataFrame
        import pandas as pd  # type: ignore
        pdf = pd.concat([f.to_backend() for f in frames]) if frames else pd.DataFrame()
        ndf = nw.from_pandas(pdf)
        # Wrap back into Frame via pandas for unified ops downstream
        return Frame(ndf.to_pandas())
    except Exception:
        import pandas as pd  # type: ignore
        return Frame(pd.concat([f.to_backend() for f in frames])) if frames else Frame(pd.DataFrame())


def groupby_agg(df: Frame) -> Frame:
    return df.groupby("cat").agg({"x": "sum", "y": "mean"})


def count_rows(df: Frame) -> int:
    return len(df.to_pandas().index)


