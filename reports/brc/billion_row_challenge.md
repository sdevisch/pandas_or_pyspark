# Billion Row Challenge (scaffold)

Generated at: 2025-09-07 11:25:49

- operation: filter
- materialize: count
- num_chunks: 1
- total_bytes: 15672

```text
backend  version       op  read_s  compute_s  rows  used_cores
-------  --------  ------  ------  ---------  ----  ----------
pandas   2.2.2     filter  0.0011     0.0024   252           1
dask     2024.5.1  filter  0.0037     0.0048   252          11
pyspark  3.5.1     filter  3.8102     0.3610   252          11
polars   1.33.0    filter  0.0026     0.0011   252           1
duckdb   1.3.2     filter  0.0139     0.0008   252           1
```
