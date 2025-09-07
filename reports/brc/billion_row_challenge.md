# Billion Row Challenge (scaffold)

Generated at: 2025-09-07 14:49:22

- operation: filter
- materialize: count
- num_chunks: 1
- total_bytes: 15672
- source: csv
- input_rows: -

```text
backend  version       op  read_s  compute_s  rows  used_cores
-------  --------  ------  ------  ---------  ----  ----------
pandas   2.2.2     filter  0.0010     0.0023   252           1
dask     2024.5.1  filter  0.0041     0.0044   252          11
pyspark  3.5.1     filter  3.8450     0.2960   252          11
polars   1.33.0    filter  0.0027     0.0012   252           1
duckdb   1.3.2     filter  0.0140     0.0008   252           1
```
