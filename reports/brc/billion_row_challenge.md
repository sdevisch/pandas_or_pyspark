# Billion Row Challenge (scaffold)

Generated at: 2025-09-07 10:25:11

- operation: filter
- materialize: head
- num_chunks: 1
- total_bytes: 15672

```text
backend  version       op  read_s  compute_s  rows  used_cores
-------  --------  ------  ------  ---------  ----  ----------
pandas   2.2.2     filter  0.0009     0.0024   252           1
dask     2024.5.1  filter  0.0040     0.0063   252          11
pyspark  3.5.1     filter  4.0072     0.2638   252          11
polars   1.33.0    filter  0.0022     0.0010   252           1
duckdb   1.3.2     filter  0.0141     0.0009   252           1
```
