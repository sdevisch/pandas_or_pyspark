# Billion Row Challenge (scaffold)

Generated at: 2025-09-07 13:35:13

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
dask     2024.5.1  filter  0.0037     0.0050   252          11
pyspark  3.5.1     filter  3.8936     0.3535   252          11
polars   1.33.0    filter  0.0026     0.0010   252           1
duckdb   1.3.2     filter  0.0142     0.0009   252           1
```
