# unipandas API demos

Generated at: 2025-09-07 15:47:55


## API demo smoke run

# unipandas benchmark

## Run context

- Data file: `/Users/sdevisch/repos/pandas_or_pyspark/data/smoke.csv`
- Ran at: 2025-09-07 15:48:01
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
backend  version   load_s  compute_s  rows  used_cores
-------  --------  ------  ---------  ----  ----------
pandas   2.2.2     0.0008     0.0000     1           1
dask     2024.5.1  0.0035     0.0085     1          11
pyspark  3.5.1     4.1603     0.4430     1          11
polars   1.33.0    0.0021     0.0000     1        None
duckdb   1.3.2     0.0086     0.0000     1        None
```

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

## Relational API demos

# Relational API demos

Generated at: 2025-09-07 15:48:26

```text
backend  version       op  load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2       join  0.1843     0.0628  2000000           1
pandas   2.2.2     concat  0.0000     0.0336  2000000           1
dask     2024.5.1    join  0.0057     0.2467  2000000          11
dask     2024.5.1  concat  0.0000     0.2052  1000000          11
pyspark  3.5.1       join  4.8954     7.5782  2000000          11
pyspark  3.5.1     concat  0.0000     5.7014  2000000          11
polars   1.33.0      join  0.0355     0.0612  2000000           1
polars   1.33.0    concat  0.0000     0.0315  2000000           1
duckdb   1.3.2       join  0.1542     0.0536  2000000           1
duckdb   1.3.2     concat  0.0000     0.0325  2000000           1
```
