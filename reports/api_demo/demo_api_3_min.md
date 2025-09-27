# unipandas API demos

Generated at: 2025-09-21 14:42:31

- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11
## API demo smoke run

Generated at: 2025-09-21 14:42:36

## Run context
- Data file: `/Users/sdevisch/repos/pandas_or_pyspark/data/smoke.csv`
- Ran at: 2025-09-21 14:42:36
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
dask     2024.5.1  0.0038     0.0081           3          11
pyspark  3.5.1     3.6885     0.1928           3          11
polars   1.33.0    0.0020     0.0000           3           1
duckdb   1.3.2     0.0085     0.0000           3           1
numpy    1.26.4    0.0007     0.0000           3           1
numba    0.60.0    0.0006     0.0000           3           1
```
- Completed bench_backends at 14:42:36


## Compatibility matrix

Generated at: 2025-09-21 14:42:43

Generated at: 2025-09-21 14:42:43
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11

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
- Completed compat_matrix at 14:42:43


## Relational API demos

Generated at: 2025-09-21 14:43:01

Generated at: 2025-09-21 14:43:01
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11

```text
backend  version   op      load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2     join    0.1718     0.0550  2000000           1
pandas   2.2.2     concat  0.0000     0.0291  2000000           1
dask     2024.5.1  join    0.0048     0.2351  2000000          11
dask     2024.5.1  concat  0.0000     0.2005  1000000          11
pyspark  3.5.1     join    4.3935     6.3719  2000000          11
pyspark  3.5.1     concat  0.0000     5.3478  2000000          11
polars   1.33.0    join    0.0209     0.0552  2000000           1
polars   1.33.0    concat  0.0000     0.0309  2000000           1
duckdb   1.3.2     join    0.1192     0.0497  2000000           1
duckdb   1.3.2     concat  0.0000     0.0293  2000000           1
```
- Completed relational_bench at 14:43:01


## BRC smoke (groupby)

Generated at: 2025-09-21 14:43:06

```text
backend  compute_s  groups
-------  ---------  ------
dask        0.0166       3
duckdb      0.0009       3
numba       0.0006       3
numpy       0.0007       3
pandas      0.0008       3
polars      0.0012       3
pyspark     1.1300       3
```

## 1-minute BRC capacity (per backend)

Generated at: 2025-09-21 14:10:44

Generated at: 2025-09-21 14:10:44

```text
backend  max_rows_within_1min  elapsed_s_at_max
-------  --------------------  ----------------
pandas                0.0e+00             0.000
dask                  0.0e+00             0.000
pyspark               0.0e+00             0.000
polars                0.0e+00             0.000
duckdb                0.0e+00             0.000
numpy                 0.0e+00             0.000
numba                 1.0e+03             4.980
```
