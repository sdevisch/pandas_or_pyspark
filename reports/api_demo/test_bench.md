# unipandas benchmark

Generated at: 2025-09-21 14:36:28

## Run context
- Data file: `/private/var/folders/yv/qzkfbsw13_q1kpm5kdb1qpx40000gn/T/pytest-of-sdevisch/pytest-5/test_run_all_scripts0/csv/tiny.csv`
- Ran at: 2025-09-21 14:36:28
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11

## Backend availability

| backend | version | status |
|---|---|---|
| pandas | 2.2.2 | available |
| dask | 2024.5.1 | available |
| pyspark | 3.5.1 | available |
| polars | 1.33.0 | available |
| duckdb | 1.3.2 | available |
| numpy | 1.26.4 | available |
| numba | 0.60.0 | available |

## Results (seconds)

```text
backend  version   load_s  compute_s  input_rows  used_cores
-------  --------  ------  ---------  ----------  ----------
pandas   2.2.2     0.0008     0.0000           3           1
dask     2024.5.1  0.0038     0.0052           3          11
pyspark  3.5.1     3.5964     0.2050           3          11
polars   1.33.0    0.0026     0.0000           3           1
duckdb   1.3.2     0.0094     0.0000           3           1
numpy    1.26.4    0.0008     0.0000           3           1
numba    0.60.0    0.0005     0.0000           3           1
```
