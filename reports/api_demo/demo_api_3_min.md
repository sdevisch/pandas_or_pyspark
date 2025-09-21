# unipandas API demos

Generated at: 2025-09-21 12:26:54

- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11
## API demo smoke run

Generated at: 2025-09-21 12:26:59

## Run context
- Data file: `/Users/sdevisch/repos/pandas_or_pyspark/data/smoke.csv`
- Ran at: 2025-09-21 12:26:59
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
dask     2024.5.1  0.0035     0.0083           3          11
pyspark  3.5.1     3.7517     0.1961           3          11
polars   1.33.0    0.0019     0.0000           3           1
duckdb   1.3.2     0.0089     0.0000           3           1
numpy    1.26.4    0.0006     0.0000           3           1
numba    0.60.0    0.0006     0.0000           3           1
```
- Completed bench_backends at 12:26:59


## Compatibility matrix

Generated at: 2025-09-21 12:27:05

Generated at: 2025-09-21 12:27:05
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
- Completed compat_matrix at 12:27:05


## Relational API demos

Generated at: 2025-09-21 12:27:23

Generated at: 2025-09-21 12:27:23
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11

```text
backend  version   op      load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2     join    0.1701     0.0566  2000000           1
pandas   2.2.2     concat  0.0000     0.0301  2000000           1
dask     2024.5.1  join    0.0049     0.2340  2000000          11
dask     2024.5.1  concat  0.0000     0.2006  1000000          11
pyspark  3.5.1     join    4.2331     6.5454  2000000          11
pyspark  3.5.1     concat  0.0000     5.1680  2000000          11
polars   1.33.0    join    0.0212     0.0575  2000000           1
polars   1.33.0    concat  0.0000     0.0312  2000000           1
duckdb   1.3.2     join    0.1155     0.0510  2000000           1
duckdb   1.3.2     concat  0.0000     0.0296  2000000           1
```
- Completed relational_bench at 12:27:23


## BRC smoke (groupby)

Generated at: 2025-09-21 12:27:28

```text
backend  compute_s  groups
-------  ---------  ------
dask        0.0142       3
duckdb      0.0005       3
numba       0.0004       3
numpy       0.0004       3
pandas      0.0004       3
polars      0.0008       3
pyspark     1.0673       3
```
