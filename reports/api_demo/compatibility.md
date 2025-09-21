# Compatibility matrix

Generated at: 2025-09-21 14:38:56

Generated at: 2025-09-21 14:38:56
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
