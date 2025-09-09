# unipandas API demos

Generated at: 2025-09-08 18:17:44

- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11


## API demo smoke run

# unipandas benchmark

## Run context

- Data file: `/Users/sdevisch/repos/pandas_or_pyspark/data/smoke.csv`
- Ran at: 2025-09-08 18:17:49
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- System available cores: 11
- Args: assign=True, query='a > 0', groupby='cat'

## Backend availability

| backend | version | status |
|---|---|---|
| pandas | 2.2.2 | available |
| dask | 2024.5.1 | available |
| pyspark | 3.5.1 | available |
| polars | 1.33.0 | available |
| duckdb | 1.3.2 | available |

## Results (seconds)

```text
backend  version   load_s  compute_s  input_rows  used_cores
-------  --------  ------  ---------  ----------  ----------
pandas   2.2.2     0.0009     0.0000           3           1
dask     2024.5.1  0.0032     0.0209           3          11
pyspark  3.5.1     3.6721     0.7425           3          11
polars   1.33.0    0.0032     0.0000           3          11
duckdb   1.3.2     0.0097     0.0000           3          11
```
- Completed bench_backends at 18:17:49


## Compatibility matrix

# Compatibility matrix

Generated at: 2025-09-08 18:17:49

```text
  operation    pandas  dask  pyspark  polars  duckdb
  -----------  ------  ----  -------  ------  ------
  select           ok    ok       ok      ok      ok
  query            ok    ok       ok      ok      ok
  assign           ok    ok       ok      ok      ok
  groupby_agg      ok    ok       ok      ok      ok
  merge            ok    ok       ok      ok      ok
  sort_values      ok    ok       ok      ok      ok
  dropna           ok    ok       ok      ok      ok
  fillna           ok    ok       ok      ok      ok
  rename           ok    ok       ok      ok      ok
  astype           ok    ok       ok      ok      ok
```
- Completed compat_matrix at 18:17:56


## Relational API demos

# Relational API demos

Generated at: 2025-09-08 18:18:14

```text
backend  version       op  load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2       join  0.1744     0.0560  2000000           1
pandas   2.2.2     concat  0.0000     0.0314  2000000           1
dask     2024.5.1    join  0.0064     0.2349  2000000          11
dask     2024.5.1  concat  0.0000     0.2002  1000000          11
pyspark  3.5.1       join  4.3305     6.8094  2000000          11
pyspark  3.5.1     concat  0.0000     5.2536  2000000          11
polars   1.33.0      join  0.0220     0.0571  2000000           1
polars   1.33.0    concat  0.0000     0.0303  2000000           1
duckdb   1.3.2       join  0.1196     0.0483  2000000           1
duckdb   1.3.2     concat  0.0000     0.0281  2000000           1
```
- Completed relational_bench at 18:18:14


## Skipped due to 3-minute budget
- brc_one_minute_runner (timeout)
- billion_row_om_runner (timeout)

