# Billion Row Challenge (scaffold) - filter

Generated at: 2025-09-08 17:54:06

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
pandas   2.2.2     filter  0.0010     0.0030   252           1
dask     2024.5.1  filter  0.0037     0.0049   252          11
pyspark  3.5.1     filter  3.6911     0.2879   252          11
polars   1.33.0    filter  0.0031     0.0010   252           1
duckdb   1.3.2     filter  0.0137     0.0008   252           1
```
