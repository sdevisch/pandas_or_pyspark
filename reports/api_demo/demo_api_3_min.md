# unipandas API demos

Generated at: 2025-09-20 10:03:31

- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11


## API demo smoke run

# unipandas benchmark

Generated at: 2025-09-20 10:03:36

## Run context
- Data file: `/Users/sdevisch/repos/pandas_or_pyspark/data/smoke.csv`
- Ran at: 2025-09-20 10:03:36
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11
- Args: assign=True, query='a > 0', groupby='cat'

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
pandas   2.2.2     0.0007     0.0000           3           1
dask     2024.5.1  0.0033     0.0206           3          11
pyspark  3.5.1     3.6730     0.7314           3          11
polars   1.33.0    0.0022     0.0000           3           1
duckdb   1.3.2     0.0084     0.0000           3           1
numpy    1.26.4    0.0005     0.0000           3           1
numba    0.60.0    0.0004     0.0000           3           1
```
- Completed bench_backends at 10:03:37


## Compatibility matrix

# Compatibility matrix

Generated at: 2025-09-20 10:03:43

Generated at: 2025-09-20 10:03:37

```text
operation    pandas  dask  pyspark  polars  duckdb  numpy  numba
-----------  ------  ----  -------  ------  ------  -----  -----
select           ok    ok       ok      ok      ok     ok     ok
query            ok    ok       ok      ok      ok     ok     ok
assign           ok    ok       ok      ok      ok     ok     ok
groupby_agg      ok    ok       ok      ok      ok   fail     ok
merge            ok    ok       ok      ok      ok     ok     ok
sort_values      ok    ok       ok      ok      ok     ok     ok
dropna           ok    ok       ok      ok      ok     ok     ok
fillna           ok    ok       ok      ok      ok     ok     ok
rename           ok    ok       ok      ok      ok     ok     ok
astype           ok    ok       ok      ok      ok     ok     ok
```
- Completed compat_matrix at 10:03:43


## Relational API demos

# Relational API demos

Generated at: 2025-09-20 10:04:01

Generated at: 2025-09-20 10:04:01

```text
backend  version   op      load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2     join    0.1728     0.0579  2000000           1
pandas   2.2.2     concat  0.0000     0.0296  2000000           1
dask     2024.5.1  join    0.0056     0.2777  2000000          11
dask     2024.5.1  concat  0.0000     0.1994  1000000          11
pyspark  3.5.1     join    4.2980     6.4626  2000000          11
pyspark  3.5.1     concat  0.0000     5.3334  2000000          11
polars   1.33.0    join    0.0217     0.0580  2000000           1
polars   1.33.0    concat  0.0000     0.0313  2000000           1
duckdb   1.3.2     join    0.1146     0.0506  2000000           1
duckdb   1.3.2     concat  0.0000     0.0305  2000000           1
```
- Completed relational_bench at 10:04:01


## BRC smoke (groupby)

# Billion Row Challenge (from JSONL)

Generated at: 2025-09-20 10:04:06

```text
backend  compute_s  groups
-------  ---------  ------
dask        0.0141       3
duckdb      0.0006       3
numba       0.0004       3
numpy       0.0005       3
pandas      0.0004       3
polars      0.0005       3
pyspark     1.0589       3
```

## Skipped due to 3-minute budget
- brc_one_minute_runner (timeout)
- billion_row_om_runner (timeout)

