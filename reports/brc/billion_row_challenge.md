# Billion Row Challenge (scaffold)

Generated at: 2025-09-07 10:31:37

- operation: filter
- materialize: head
- num_chunks: 1
- total_bytes: 15672

```text
backend  version       op  read_s  compute_s  rows  used_cores
-------  --------  ------  ------  ---------  ----  ----------
pandas   2.2.2     filter  0.0011     0.0024   252           1
dask     2024.5.1  filter  0.0036     0.0064   252          11
pyspark  3.5.1     filter  4.9607     0.2637   252          11
polars   1.33.0    filter  0.0022     0.0009   252           1
duckdb   1.3.2     filter  0.0138     0.0010   252           1
```
