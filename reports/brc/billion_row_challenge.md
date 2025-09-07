# Billion Row Challenge (scaffold)

Generated at: 2025-09-07 11:45:28

- operation: filter
- materialize: count
- num_chunks: 1
- total_bytes: 15672
- source: csv
- input_rows: -

```text
backend  version       op  read_s  compute_s  rows  used_cores
-------  --------  ------  ------  ---------  ----  ----------
pandas   2.2.2     filter  0.0010     0.0026   252           1
dask     2024.5.1  filter  0.0038     0.0045   252          11
pyspark  3.5.1     filter  3.7805     0.3566   252          11
polars   1.33.0    filter  0.0028     0.0014   252           1
duckdb   1.3.2     filter  0.0134     0.0009   252           1
```
