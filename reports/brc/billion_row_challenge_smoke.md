# Billion Row Challenge (scaffold) - filter

Generated at: 2025-09-08 18:21:21

- operation: filter
- materialize: count
- num_chunks: 1
- total_bytes: 15672
- source: csv
- input_rows: -
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11

```text
backend  version       op  read_s  compute_s  rows  used_cores
-------  --------  ------  ------  ---------  ----  ----------
pandas   2.2.2     filter  0.0019     0.0027   252           1
dask     2024.5.1  filter  0.0037     0.0046   252          11
pyspark  3.5.1     filter  4.0513     0.3509   252          11
polars   1.33.0    filter  0.0036     0.0013   252           1
duckdb   1.3.2     filter  0.0145     0.0008   252           1
```
