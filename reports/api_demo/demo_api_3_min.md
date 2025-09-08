# unipandas API demos

Generated at: 2025-09-08 16:21:19

- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11


## API demo smoke run

# unipandas benchmark

## Run context

- Data file: `/Users/sdevisch/repos/pandas_or_pyspark/data/smoke.csv`
- Ran at: 2025-09-08 16:21:25
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
pandas   2.2.2     0.0025     0.0000           3           1
dask     2024.5.1  0.0077     0.0211           3          11
pyspark  3.5.1     4.2984     0.7665           3          11
polars   1.33.0    0.0149     0.0000           3          11
duckdb   1.3.2     0.0467     0.0000           3          11
```
- Completed bench_backends at 16:21:25


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
- Completed compat_matrix at 16:21:31


## Relational API demos

# Relational API demos

Generated at: 2025-09-08 16:21:49

```text
backend  version       op  load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2       join  0.1769     0.0602  2000000           1
pandas   2.2.2     concat  0.0000     0.0321  2000000           1
dask     2024.5.1    join  0.0046     0.2404  2000000          11
dask     2024.5.1  concat  0.0000     0.2017  1000000          11
pyspark  3.5.1       join  4.3958     6.4000  2000000          11
pyspark  3.5.1     concat  0.0000     5.2510  2000000          11
polars   1.33.0      join  0.0238     0.0569  2000000           1
polars   1.33.0    concat  0.0000     0.0318  2000000           1
duckdb   1.3.2       join  0.1152     0.0522  2000000           1
duckdb   1.3.2     concat  0.0000     0.0306  2000000           1
```
- Completed relational_bench at 16:21:49


## Skipped due to 3-minute budget
- brc_one_minute_runner (timeout)
- billion_row_om_runner (timeout)

