# unipandas API demos

Generated at: 2025-09-08 16:45:17

- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11


## API demo smoke run

# unipandas benchmark

## Run context

- Data file: `/Users/sdevisch/repos/pandas_or_pyspark_fresh/data/smoke.csv`
- Ran at: 2025-09-08 16:45:24
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
pandas   2.2.2     0.0008     0.0000           3           1
dask     2024.5.1  0.0051     0.0202           3          11
pyspark  3.5.1     4.4172     0.7466           3          11
polars   1.33.0    0.0139     0.0000           3          11
duckdb   1.3.2     0.0466     0.0000           3          11
```
- Completed bench_backends at 16:45:24


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
- Completed compat_matrix at 16:45:30


## Relational API demos

# Relational API demos

Generated at: 2025-09-08 16:45:49

```text
backend  version       op  load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2       join  0.2482     0.0570  2000000           1
pandas   2.2.2     concat  0.0000     0.0314  2000000           1
dask     2024.5.1    join  0.0052     0.2288  2000000          11
dask     2024.5.1  concat  0.0000     0.1961  1000000          11
pyspark  3.5.1       join  4.3042     6.5151  2000000          11
pyspark  3.5.1     concat  0.0000     5.2438  2000000          11
polars   1.33.0      join  0.0241     0.0554  2000000           1
polars   1.33.0    concat  0.0000     0.0298  2000000           1
duckdb   1.3.2       join  0.1140     0.0506  2000000           1
duckdb   1.3.2     concat  0.0000     0.0288  2000000           1
```
- Completed relational_bench at 16:45:49


## Skipped due to 3-minute budget
- brc_one_minute_runner (timeout)
- billion_row_om_runner (timeout)

