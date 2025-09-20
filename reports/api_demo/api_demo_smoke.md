# unipandas benchmark

Generated at: 2025-09-20 07:37:48

## Run context
- Data file: `/Users/sdevisch/repos/pandas_or_pyspark/data/bench_1000000.csv`
- Ran at: 2025-09-20 07:37:48
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11
- Args: assign=False, query=None, groupby='cat'

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
pandas   2.2.2     0.0890     0.0000     1000000           1
dask     2024.5.1  0.0065     0.2934     1000000          11
pyspark  3.5.1     4.0251     1.3363     1000000          11
polars   1.33.0    0.0235     0.0000     1000000           1
duckdb   1.3.2     0.1020     0.0000     1000000           1
numpy    1.26.4    0.1312     0.0000     1000000           1
numba    0.60.0    0.0872     0.0100     1000000           1
```
