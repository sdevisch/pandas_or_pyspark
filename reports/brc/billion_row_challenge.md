# Billion Row Challenge (scaffold) - filter

Generated at: 2025-09-20 07:36:53

- operation: filter
- materialize: count
- num_chunks: 1
- total_bytes: 17397
- source: parquet
- input_rows: 1000
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11

```text
backend  version  op      read_s  compute_s  rows  used_cores
-------  -------  ------  ------  ---------  ----  ----------
pandas   2.2.2    filter  0.0018     0.0006   245           1
```
