# unipandas API demos

Generated at: 2025-09-20 13:13:20

- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11


## API demo smoke run

# unipandas benchmark

Generated at: 2025-09-20 13:13:25

## Run context
- Data file: `/Users/sdevisch/repos/pandas_or_pyspark/data/smoke.csv`
- Ran at: 2025-09-20 13:13:25
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
pandas   2.2.2     0.0016     0.0000           3           1
dask     2024.5.1  0.0036     0.0077           3          11
pyspark  3.5.1     3.6726     0.1770           3          11
polars   1.33.0    0.0019     0.0000           3           1
duckdb   1.3.2     0.0084     0.0000           3           1
numpy    1.26.4    0.0006     0.0000           3           1
numba    0.60.0    0.0005     0.0000           3           1
```
- Completed bench_backends at 13:13:26


## Compatibility matrix

# Compatibility matrix

Generated at: 2025-09-20 13:13:32

Generated at: 2025-09-20 13:13:26

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
- Completed compat_matrix at 13:13:32


## Relational API demos

# Relational API demos

Generated at: 2025-09-20 13:13:50

Generated at: 2025-09-20 13:13:50

```text
backend  version   op      load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2     join    0.1724     0.0520  2000000           1
pandas   2.2.2     concat  0.0000     0.0314  2000000           1
dask     2024.5.1  join    0.0049     0.2345  2000000          11
dask     2024.5.1  concat  0.0000     0.1998  1000000          11
pyspark  3.5.1     join    4.2828     6.3107  2000000          11
pyspark  3.5.1     concat  0.0000     5.4997  2000000          11
polars   1.33.0    join    0.0215     0.0554  2000000           1
polars   1.33.0    concat  0.0000     0.0315  2000000           1
duckdb   1.3.2     join    0.1155     0.0505  2000000           1
duckdb   1.3.2     concat  0.0000     0.0303  2000000           1
```
- Completed relational_bench at 13:13:50


## BRC smoke (groupby)

# Billion Row Challenge (from JSONL)

Generated at: 2025-09-20 13:13:55

```text
backend  compute_s  groups
-------  ---------  ------
dask        0.0142       3
duckdb      0.0005       3
numba       0.0004       3
numpy       0.0005       3
pandas      0.0004       3
polars      0.0005       3
pyspark     1.0503       3
```

## Skipped due to 3-minute budget
- brc_one_minute_runner (timeout)
- billion_row_om_runner (timeout)

