unipandas
=========

Unified pandas-like API that runs with these backends:
- pandas (default)
- Dask DataFrame
- pandas API on Spark (pyspark.pandas)

Select backend via environment variable or at runtime.

Install
-------

Base package contains no heavy dependencies. Install your desired backend extras:

```bash
pip install -e .
# then choose one or more backends
pip install '.[pandas]'
pip install '.[dask]'
pip install '.[pyspark]'
```

Usage
-----

Choose backend (env var or API):

```bash
export UNIPANDAS_BACKEND=pandas    # or dask, pyspark
```

```python
from unipandas import configure_backend, read_csv
configure_backend("dask")

df = read_csv("data.csv")  # returns Frame wrapper
print(df.backend)

# Unified ops
out = (
    df.select(["a", "b"])  # column projection
      .query("a > 0")        # row filter
      .assign(c=lambda x: x["a"] + x["b"])  # add column
)

agg = out.groupby("a").agg({"c": "sum"})
print(agg.head().to_pandas())
```

API surface (initial)
---------------------
- `read_csv`, `read_parquet` → `Frame`
- `Frame.select`, `Frame.query`, `Frame.assign`
- `Frame.groupby(...).agg(...)`
- `Frame.merge`
- `Frame.head`, `Frame.to_pandas`, `Frame.to_backend`

Notes
-----
- Dask operations are lazy; call `to_pandas()` to compute.
- pandas-on-Spark mirrors pandas closely; `to_pandas()` brings data locally.

License
-------
MIT


