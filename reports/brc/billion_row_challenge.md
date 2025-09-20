# Billion Row Challenge (scaffold) - groupby

Generated at: 2025-09-20 07:49:11

- operation: groupby
- materialize: count
- num_chunks: 1
- total_bytes: 9547
- source: parquet
- input_rows: 1000
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11

```text
backend  version   op       read_s  compute_s  rows  used_cores  groups
-------  --------  -------  ------  ---------  ----  ----------  ------
pandas   2.2.2     groupby  0.0019     0.0004  1000           1       3
dask     2024.5.1  groupby  0.0048     0.0131  1000          11       3
pyspark  3.5.1     groupby  3.4874     1.0164  1000          11       3
polars   1.33.0    groupby  0.0223     0.0005  1000           1       3
duckdb   1.3.2     groupby  0.0093     0.0005  1000           1       3
numpy    1.26.4    groupby  0.0013     0.0004  1000           1       3
numba    0.60.0    groupby  0.0010     0.0006  1000           1       3
```

Groupby result preview (by backend):

```text
backend  cat   x                     y
-------  ---  --  --------------------
pandas   x     1  -0.11246200607902736
pandas   y    32   0.28695652173913044
pandas   z    -3   0.24539877300613497
backend  cat   x                     y
-------  ---  --  --------------------
dask     x     1  -0.11246200607902736
dask     y    32   0.28695652173913044
dask     z    -3   0.24539877300613497
backend  cat   x                     y
-------  ---  --  --------------------
pyspark  x     1  -0.11246200607902736
pyspark  y    32   0.28695652173913044
pyspark  z    -3   0.24539877300613497
backend  cat   x                     y
-------  ---  --  --------------------
polars   x     1  -0.11246200607902736
polars   y    32   0.28695652173913044
polars   z    -3   0.24539877300613497
backend  cat   x                     y
-------  ---  --  --------------------
duckdb   x     1  -0.11246200607902736
duckdb   y    32   0.28695652173913044
duckdb   z    -3   0.24539877300613497
backend  cat   x                     y
-------  ---  --  --------------------
numpy    x     1  -0.11246200607902736
numpy    y    32   0.28695652173913044
numpy    z    -3   0.24539877300613497
backend  cat   x                     y
-------  ---  --  --------------------
numba    x     1  -0.11246200607902736
numba    y    32   0.28695652173913044
numba    z    -3   0.24539877300613497
```
