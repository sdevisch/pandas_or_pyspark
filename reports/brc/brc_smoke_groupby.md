# Billion Row Challenge (scaffold) - groupby

Generated at: 2025-09-20 13:38:48

- operation: groupby
- materialize: count
- num_chunks: 1
- total_bytes: 17397
- source: parquet
- input_rows: 1000
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11

```text
backend  version   op       read_s  compute_s  rows  used_cores  groups
-------  --------  -------  ------  ---------  ----  ----------  ------
numba    0.60.0    groupby  0.0019     0.0004  1000           1       3
pandas   2.2.2     groupby       -          -     -           -       -
dask     2024.5.1  groupby       -          -     -           -       -
pyspark  3.5.1     groupby       -          -     -           -       -
polars   1.33.0    groupby       -          -     -           -       -
duckdb   1.3.2     groupby       -          -     -           -       -
numpy    1.26.4    groupby       -          -     -           -       -
```

Groupby result preview (by backend):

```text
backend  cat      x                   y
-------  ---  -----  ------------------
numba    x    -8780   53.38291139240506
numba    y     7896  2.1432664756446993
numba    z     3211   9.814925373134328
backend  cat      x                   y
-------  ---  -----  ------------------
pandas   x    -8780   53.38291139240506
pandas   y     7896  2.1432664756446993
pandas   z     3211   9.814925373134328
backend  cat      x                   y
-------  ---  -----  ------------------
dask     x    -8780   53.38291139240506
dask     y     7896  2.1432664756446993
dask     z     3211   9.814925373134328
backend  cat      x                   y
-------  ---  -----  ------------------
pyspark  x    -8780   53.38291139240506
pyspark  y     7896  2.1432664756446993
pyspark  z     3211   9.814925373134328
backend  cat      x                   y
-------  ---  -----  ------------------
polars   x    -8780   53.38291139240506
polars   y     7896  2.1432664756446993
polars   z     3211   9.814925373134328
backend  cat      x                   y
-------  ---  -----  ------------------
duckdb   x    -8780   53.38291139240506
duckdb   y     7896  2.1432664756446993
duckdb   z     3211   9.814925373134328
backend  cat      x                   y
-------  ---  -----  ------------------
numpy    x    -8780   53.38291139240506
numpy    y     7896  2.1432664756446993
numpy    z     3211   9.814925373134328
```
