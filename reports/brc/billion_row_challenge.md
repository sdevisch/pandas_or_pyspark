# Billion Row Challenge (scaffold)

Generated at: 2025-09-07 10:19:03

- operation: filter
- materialize: head
- num_chunks: 1
- total_bytes: 15672

```text
backend  version       op  read_s  compute_s  rows  used_cores
-------  --------  ------  ------  ---------  ----  ----------
pandas   2.2.2     filter  0.0010     0.0027   252           1
dask     2024.5.1  filter  0.0035     0.0063   252          11
pyspark  3.5.1     filter  8.3510     0.2560   252          11
polars   1.33.0    filter  0.0021     0.0009   252           1
duckdb   1.3.2     filter  0.0135     0.0009   252           1
```
