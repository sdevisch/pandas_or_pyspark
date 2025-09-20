unipandas
=========

This repository serves three purposes:

1. Demonstrate which pandas syntax different backends share, and where they differ (with workarounds).
2. Run API demos across backends to illustrate behavior and performance.
3. Run a Billion Row Challenge (BRC) with safe scaffolding and an order-of-magnitude runner.

Unified pandas-like API that runs with these backends:
- pandas (default)
- Dask DataFrame
- pandas API on Spark (pyspark.pandas)
- Polars
- DuckDB
- NumPy (experimental)
- Numba (experimental)

Select backend via environment variable or at runtime.

Why a pandas-first API?
-----------------------

Pandas syntax is a de facto standard for tabular data processing. Many engines implement
some level of pandas compatibility (Dask, pandas-on-Spark, Polars, DuckDB), so writing
code once in a pandas-like dialect gives you portability across local laptops and
distributed grids.

- Single mental model: author your logic as pandas-style transformations.
- Swap engines without rewriting: choose a backend at runtime for scale/perf needs.
- Measure and learn: the same pipeline can be timed across engines to inform choices.

Where this lives in the codebase
--------------------------------

- `src/unipandas/frame.py`: the unified `Frame` wrapper exposing pandas-like operations
  (`select`, `query`, `assign`, `groupby(...).agg(...)`, `merge`, `head`, `to_pandas`, `to_backend`).
- `src/unipandas/io.py`: backend-aware `read_csv` / `read_parquet` that return a `Frame`.
- `src/unipandas/backend.py`: backend configuration and detection (via `configure_backend` or env var).
- `scripts/utils.py`: canonical list of supported backends and helpers (availability, versions, markdown formatting).
- `scripts/api_demo/bench_backends.py`: demonstrates the exact same pandas-style pipeline across all backends with timing.

How to run locally vs on grids
------------------------------

- Local (pandas):
  - `export UNIPANDAS_BACKEND=pandas`
  - `python scripts/api_demo/bench_backends.py data/example.csv --assign --query "a > 0" --groupby cat --md-out reports/api_demo/benchmark.md`

- Dask (local cluster):
  - `export UNIPANDAS_BACKEND=dask`
  - `python scripts/api_demo/bench_backends.py data/example.csv --assign --query "a > 0" --groupby cat --md-out reports/api_demo/benchmark.md`

- Spark (pandas API on Spark):
  - `export UNIPANDAS_BACKEND=pyspark`
  - `export PYARROW_IGNORE_TIMEZONE=1`
  - `python scripts/api_demo/bench_backends.py data/example.csv --assign --query "a > 0" --groupby cat --md-out reports/api_demo/benchmark.md`

Future: benchmark-driven delegation
-----------------------------------

Because the same pandas-style pipeline can be executed across engines, we can record
performance under different data sizes and shapes and, in the future, implement a
"router" that selects the engine per workload (size, operation mix, environment) to
maximize performance—without changing user code. The API surface stays pandas-like;
the engine selection becomes an implementation detail informed by benchmarks.

Install
-------

Base package contains no heavy dependencies. Install your desired backend extras:

```bash
pip install -e .
# then choose one or more backends
pip install '.[pandas]'
pip install '.[dask]'
pip install '.[pyspark]'
pip install '.[polars]'
pip install '.[duckdb]'
```

Usage
-----

Choose backend (env var or API):

```bash
export UNIPANDAS_BACKEND=pandas    # or dask, pyspark, polars, duckdb, numpy, numba
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
Run the same workload across available backends and compare timings.

Front-ends vs Backends
----------------------
- Front-end: the user-facing API driving the workload (pandas, PySpark's pandas-on-Spark, Narwhals).
- Backend: the compute engine executing the work (pandas, Dask, PySpark, Polars, DuckDB, …).

We explore multiple front-ends to understand ergonomics and performance portability across backends.

```bash
python scripts/api_demo/bench_backends.py path/to/data.csv --assign --query "a > 0" --groupby a
# Perf matrix (JSONL → Markdown), exploring front-ends:
python scripts/perf/measure.py --frontends pandas,pyspark,narwhals --backends pandas,polars,duckdb --glob "data/brc_scales/parquet_1000000/*.parquet" --out results/perf.jsonl
python scripts/reports/perf_matrix.py --in results/perf.jsonl --out reports/perf/perf_matrix.md
```

Repository structure
--------------------

- Core library (put importable, reusable code here)
  - `src/unipandas/backend.py`: backend detection/configuration (env var and API)
  - `src/unipandas/frame.py`: thin `Frame` wrapper exposing a unified subset (select, query, assign, groupby/agg, merge, head, to_pandas)
  - `src/unipandas/io.py`: IO helpers (`read_csv`, `read_parquet`) returning `Frame`

- Scripts (CLI tools, demos and runners)
  - API demos
    - `scripts/api_demo/bench_backends.py`: time the same workload across pandas, Dask, pandas-on-Spark; writes Markdown via `--md-out`
    - `scripts/api_demo/relational_bench.py`: join/concat demos; writes `reports/api_demo/relational_api_demo.md`
    - `scripts/api_demo/compat_matrix.py`: generate a compatibility matrix; writes `reports/api_demo/compatibility.md`
  - Billion Row Challenge (under `scripts/brc/`, outputs in `reports/brc/`)
    - `billion_row_challenge.py`: groupby-only scaffold (Parquet-only). Routes <1B runs to `brc_smoke_groupby.md`, 1B runs to `brc_1b_groupby.md`. Supports `--jsonl-out` and `--no-md`.
    - `billion_row_om_runner.py`: order-of-magnitude runner with per-step timeout, writes `brc_order_of_magnitude.md`
    - `brc_one_minute_runner.py`: per-backend capacity under 1 minute, writes `brc_under_1min_capacity.md`
    - `brc_jsonl_pipeline.py`: orchestrates JSONL-first run then renders Markdown via reporter

- Reports (generated)
  - `reports/api_demo/demo_api_3_min.md`: aggregated results from `demo_api_3_min`
  - `reports/api_demo/api_demo_smoke.md`: smoke run summary
  - `reports/api_demo/compatibility.md`: fixed-width compatibility table
  - `reports/api_demo/relational_api_demo.md`: join/concat demo timings
  - `reports/brc/brc_1b_groupby.md`: BRC 1B groupby report (main)
  - `reports/brc/brc_smoke_groupby.md`: BRC smoke runs (<1B)
  - `reports/brc/brc_order_of_magnitude.md`: OM results
  - `reports/brc/brc_under_1min_capacity.md`: 1-minute capacity results

Quick start
-----------

1) Ensure a Python env with desired backends (see Install). For Spark:
   `export PYARROW_IGNORE_TIMEZONE=1`
2) Quick API demo across backends:
   `python scripts/api_demo/bench_backends.py data/example.csv --assign --query "a > 0" --groupby cat --md-out reports/api_demo/quick.md`

3) Billion Row Challenge (BRC): Parquet-only
   - Provide Parquet glob(s): `--data-glob "data/brc_scales/parquet_1000000/*.parquet"`
   - CSV inputs are no longer supported for BRC.

Reporting is standardized via a small internal module (`mdreport`) that writes Markdown under `reports/` at the repo root.
3) Relational workloads (join/concat):
   `python scripts/api_demo/relational_bench.py`
4) Compatibility matrix:
   `python scripts/api_demo/compat_matrix.py`
5) Billion row challenge (groupby + count), JSONL-first:
   `python scripts/brc/billion_row_challenge.py --data-glob "data/brc_scales/parquet_1000000000/*.parquet" --jsonl-out results/brc_1b_groupby.jsonl --no-md`
   Render: `python scripts/reports/brc_report_from_jsonl.py --in results/brc_1b_groupby.jsonl --out reports/brc/brc_1b_groupby.md`
6) Order-of-magnitude runner (per-step budget):
   `python scripts/brc/billion_row_om_runner.py --budgets 180`
7) One-minute capacity:
   `python scripts/brc/brc_one_minute_runner.py --budget 60`

License
-------
MIT


