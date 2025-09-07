# Scripts

This directory contains runnable tools and demos that exercise the library
under `src/unipandas/`.

- Benchmarks and compatibility
  - `bench_backends.py`: Compare load/compute timings across backends on the same workload.
  - `relational_bench.py`: Join/concat timings per backend.
  - `compat_matrix.py`: Which operations work across backends.

- Billion Row Challenge (BRC)
  - All BRC scripts live in `scripts/brc/` and write reports to `reports/brc/`.
  - `billion_row_challenge.py`: Scalable scaffold over chunked CSV/Parquet; supports `filter`/`groupby` and `head`/`count`/`all` materialization.
  - `billion_row_om_runner.py`: Order-of-magnitude runner with per-step budgets.
  - `brc_generate_data.py`, `brc_generate_all_scales.py`: Data generation utilities.
  - `brc_scale_runner.py`, `brc_one_minute_runner.py`: Convenience runners.

Tip: Export `PYARROW_IGNORE_TIMEZONE=1` for PySpark/pandas-on-Spark.


