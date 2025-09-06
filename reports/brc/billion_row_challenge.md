# Billion Row Challenge (scaffold)

Generated at: 2025-09-06 17:03:13

- operation: filter
- materialize: head
- num_chunks: 1
- total_bytes: 15672

```text
backend  version       op  read_s  compute_s  rows  used_cores
-------  --------  ------  ------  ---------  ----  ----------
pandas   2.2.2     filter  0.0011     0.0024   252           1
dask     2024.5.1  filter  0.0038     0.0058   252          11
pyspark  3.5.1     filter  3.8951     0.2572   252          11
polars   1.33.0    filter  0.0023     0.0010   252           1
duckdb   1.3.2     filter  0.0134     0.0009   252           1
```
