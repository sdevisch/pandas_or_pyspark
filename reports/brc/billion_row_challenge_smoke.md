# Billion Row Challenge (scaffold) - groupby

Generated at: 2025-09-08 16:21:54

- operation: groupby
- materialize: count
- num_chunks: 1
- total_bytes: 15672
- source: csv
- input_rows: -
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11

```text
backend  version        op  read_s  compute_s  rows  used_cores  groups
-------  --------  -------  ------  ---------  ----  ----------  ------
pandas   2.2.2     groupby  0.0017     0.0010     -           1       3
dask     2024.5.1  groupby  0.0040     0.0179     -          11       3
pyspark  3.5.1     groupby  3.8138     0.5228     -          11       3
polars   1.33.0    groupby  0.0037     0.0009     -           1       3
duckdb   1.3.2     groupby  0.0152     0.0007     -           1       3
```

Groupby result preview (by backend):

```text
backend  cat      x                   y
-------  ---  -----  ------------------
pandas   x     4186  -7.863768115942029
pandas   y    11034  -2.033132530120482
pandas   z     6640   29.51702786377709
backend  cat      x                   y
-------  ---  -----  ------------------
dask     x     4186  -7.863768115942029
dask     y    11034  -2.033132530120482
dask     z     6640   29.51702786377709
backend  cat      x                   y
-------  ---  -----  ------------------
pyspark  x     4186  -7.863768115942029
pyspark  y    11034  -2.033132530120482
pyspark  z     6640   29.51702786377709
backend  cat      x                   y
-------  ---  -----  ------------------
polars   x     4186  -7.863768115942029
polars   y    11034  -2.033132530120482
polars   z     6640   29.51702786377709
backend  cat      x                   y
-------  ---  -----  ------------------
duckdb   x     4186  -7.863768115942029
duckdb   y    11034  -2.033132530120482
duckdb   z     6640   29.51702786377709
```
