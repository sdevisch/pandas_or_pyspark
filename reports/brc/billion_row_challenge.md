# Billion Row Challenge (scaffold)

Generated at: 2025-09-07 12:34:06

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
dask     2024.5.1  filter  0.0050     0.0049   252          11
pyspark  3.5.1     filter  4.8631     0.3914   252          11
polars   1.33.0    filter  0.0030     0.0011   252           1
duckdb   1.3.2     filter  0.0143     0.0009   252           1
```
