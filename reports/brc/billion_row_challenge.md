# Billion Row Challenge (scaffold)

Generated at: 2025-09-06 14:35:07

- num_chunks: 1
- total_bytes: 15672

```text
backend  version       op  read_s  compute_s  rows  used_cores
-------  --------  ------  ------  ---------  ----  ----------
pandas   2.2.2     filter  0.0010     0.0023   252           1
dask     2024.5.1  filter  0.0037     0.0065   252          11
pyspark  3.5.1     filter  3.7821     0.2561   252          11
polars   1.33.0    filter  0.0021     0.0010   252           1
duckdb   1.3.2     filter  0.0133     0.0010   252           1
```
