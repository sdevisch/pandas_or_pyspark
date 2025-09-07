# Billion Row Challenge (scaffold)

Generated at: 2025-09-07 13:09:20

- operation: filter
- materialize: count
- num_chunks: 1
- total_bytes: 15672
- source: csv
- input_rows: -

```text
backend  version       op  read_s  compute_s  rows  used_cores
-------  --------  ------  ------  ---------  ----  ----------
pandas   2.2.2     filter  0.0013     0.0026   252           1
dask     2024.5.1  filter  0.0052     0.0056   252          11
pyspark  3.5.1     filter  4.4244     0.3896   252          11
polars   1.33.0    filter  0.0044     0.0019   252           1
duckdb   1.3.2     filter  0.0149     0.0010   252           1
```
