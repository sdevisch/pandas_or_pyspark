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

Benchmarking
------------
Run the same workload across available backends and compare timings:

```bash
python scripts/bench_backends.py path/to/data.csv --assign --query "a > 0" --groupby a
```

Project map
-----------

- Core library
  - `src/unipandas/backend.py`: backend detection/configuration (env var and API)
  - `src/unipandas/frame.py`: thin `Frame` wrapper exposing a unified subset (select, query, assign, groupby/agg, merge, head, to_pandas)
  - `src/unipandas/io.py`: IO helpers (`read_csv`, `read_parquet`) returning `Frame`

- Scripts (runnable tools)
  - `run_benchmark`: orchestrates multiple scenarios and aggregates Markdown under `reports/`
  - `scripts/bench_backends.py`: times the same workload across pandas, Dask, and pandas-on-Spark; writes Markdown via `--md-out`
  - `scripts/relational_bench.py`: larger datasets; join and concat timings per backend; writes `reports/relational_benchmark.md`
  - `scripts/compat_matrix.py`: generates a compatibility matrix showing which operations work across backends; writes `reports/compatibility.md`
  - BRC (Billion Row Challenge) scripts live under `scripts/brc/` and write to `reports/brc/`
    - `billion_row_challenge.py`: scalable scaffold for a BRC-style run (chunked Parquet/CSV, filter/groupby)
    - `billion_row_om_runner.py`: order-of-magnitude runner up to 100M rows with per-step timeout
    - `brc_generate_data.py`: generate chunked Parquet/CSV
    - `brc_generate_all_scales.py`: generate 1M/10M/100M/1B datasets with chunking
    - `brc_scale_runner.py`, `brc_one_minute_runner.py`: scale and 1-minute runners

- Reports (generated)
  - `reports/benchmark.md`: aggregated results from `run_benchmark`
  - `reports/benchmark_s1.md`, `reports/benchmark_s2.md`: per-scenario outputs
  - `reports/compatibility.md`: fixed-width compatibility table
  - `reports/relational_benchmark.md`: join/concat performance
  - `reports/billion_row_challenge.md`: BRC scaffold output
  - `reports/billion_row_om.md`: order-of-magnitude BRC results

How to run quickly
------------------

1) Ensure a Python env with desired backends (see Install). For Spark:
   `export PYARROW_IGNORE_TIMEZONE=1`
2) Quick benchmark across backends:
   `python scripts/bench_backends.py data/example.csv --assign --query "a > 0" --groupby cat --md-out reports/quick.md`
3) Relational workloads (join/concat):
   `python scripts/relational_bench.py`
4) Compatibility matrix:
   `python scripts/compat_matrix.py`
5) Billion row challenge scaffold (safe defaults):
   `python scripts/billion_row_challenge.py --rows-per-chunk 100000 --num-chunks 2 --operation filter`
6) Order-of-magnitude runner:
   `python scripts/billion_row_om_runner.py --budgets 180`

License
-------
MIT


