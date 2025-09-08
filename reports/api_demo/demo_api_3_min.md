# unipandas API demos

Generated at: 2025-09-08 16:30:19

- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11


## API demo smoke run

# unipandas benchmark

## Run context

- Data file: `/Users/sdevisch/repos/pandas_or_pyspark/data/smoke.csv`
- Ran at: 2025-09-08 16:30:25
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
pandas   2.2.2     0.0010     0.0000           3           1
dask     2024.5.1  0.0038     0.0214           3          11
pyspark  3.5.1     4.1005     0.7475           3          11
polars   1.33.0    0.0146     0.0000           3          11
duckdb   1.3.2     0.0478     0.0000           3          11
```
- Completed bench_backends at 16:30:25


## Compatibility matrix

# Compatibility matrix

Generated at: 2025-09-07 09:23:35

```text
  operation    pandas  dask  pyspark
  -----------  ------  ----  -------
  select           ok    ok       ok
  query            ok    ok       ok
  assign           ok    ok       ok
  groupby_agg      ok    ok       ok
  merge            ok    ok       ok
```
- Completed compat_matrix at 16:30:32


## Relational API demos

# Relational API demos

Generated at: 2025-09-08 16:30:49

```text
backend  version       op  load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2       join  0.1766     0.0573  2000000           1
pandas   2.2.2     concat  0.0000     0.0307  2000000           1
dask     2024.5.1    join  0.0049     0.2384  2000000          11
dask     2024.5.1  concat  0.0000     0.2010  1000000          11
pyspark  3.5.1       join  4.3432     6.3207  2000000          11
pyspark  3.5.1     concat  0.0000     5.3065  2000000          11
polars   1.33.0      join  0.0242     0.0549  2000000           1
polars   1.33.0    concat  0.0000     0.0311  2000000           1
duckdb   1.3.2       join  0.1143     0.0487  2000000           1
duckdb   1.3.2     concat  0.0000     0.0282  2000000           1
```
- Completed relational_bench at 16:30:49


## Skipped due to 3-minute budget
- brc_one_minute_runner (timeout)
- billion_row_om_runner (timeout)

