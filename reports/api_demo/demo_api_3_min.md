# unipandas API demos

Generated at: 2025-09-08 17:51:46

- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11


## API demo smoke run

# unipandas benchmark

## Run context

- Data file: `/Users/sdevisch/repos/pandas_or_pyspark/data/smoke.csv`
- Ran at: 2025-09-08 17:51:53
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
pandas   2.2.2     0.0017     0.0000           3           1
dask     2024.5.1  0.0069     0.0261           3          11
pyspark  3.5.1     4.6723     0.7514           3          11
polars   1.33.0    0.0160     0.0000           3          11
duckdb   1.3.2     0.0506     0.0000           3          11
```
- Completed bench_backends at 17:51:53


## Compatibility matrix

# Compatibility matrix

Generated at: 2025-09-08 17:51:53

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
- Completed compat_matrix at 17:51:59


## Relational API demos

# Relational API demos

Generated at: 2025-09-08 17:52:17

```text
backend  version       op  load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2       join  0.1736     0.0569  2000000           1
pandas   2.2.2     concat  0.0000     0.0327  2000000           1
dask     2024.5.1    join  0.0059     0.2434  2000000          11
dask     2024.5.1  concat  0.0000     0.2052  1000000          11
pyspark  3.5.1       join  4.3620     6.4657  2000000          11
pyspark  3.5.1     concat  0.0000     5.3969  2000000          11
polars   1.33.0      join  0.0245     0.0576  2000000           1
polars   1.33.0    concat  0.0000     0.0308  2000000           1
duckdb   1.3.2       join  0.1158     0.0503  2000000           1
duckdb   1.3.2     concat  0.0000     0.0304  2000000           1
```
- Completed relational_bench at 17:52:18


## Skipped due to 3-minute budget
- brc_one_minute_runner (timeout)
- billion_row_om_runner (timeout)

