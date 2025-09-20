# Billion Row Challenge (scaffold) - groupby

Generated at: 2025-09-20 07:46:04

- operation: groupby
- materialize: count
- num_chunks: 1
- total_bytes: 17397
- source: parquet
- input_rows: 1000
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11

```text
backend  version  op       read_s  compute_s  rows  used_cores  groups
-------  -------  -------  ------  ---------  ----  ----------  ------
pandas   2.2.2    groupby  0.0020     0.0004  1000           1       3
```

Groupby result preview (by backend):

```text
backend  cat      x                   y
-------  ---  -----  ------------------
pandas   x    -8780   53.38291139240506
pandas   y     7896  2.1432664756446993
pandas   z     3211   9.814925373134328
```
