# Billion Row Challenge (scaffold)

Generated at: 2025-09-07 11:42:16

- operation: filter
- materialize: count
- num_chunks: 1
- total_bytes: 15672
- source: csv
- input_rows: -

```text
backend  version       op  read_s  compute_s  rows  used_cores
-------  --------  ------  ------  ---------  ----  ----------
pandas   2.2.2     filter  0.0011     0.0024   252           1
dask     2024.5.1  filter  0.0038     0.0052   252          11
pyspark  3.5.1     filter  3.8947     0.3514   252          11
polars   1.33.0    filter  0.0026     0.0010   252           1
duckdb   1.3.2     filter  0.0137     0.0008   252           1
```
