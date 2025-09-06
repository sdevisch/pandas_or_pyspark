# Billion Row Challenge (scaffold)

Generated at: 2025-09-06 16:39:11

- num_chunks: 1
- total_bytes: 166761

```text
backend  version       op  read_s  compute_s  rows  used_cores
-------  --------  ------  ------  ---------  ----  ----------
pandas   2.2.2     filter  0.0021     0.0026  2522           1
dask     2024.5.1  filter  0.0038     0.0080  2522          11
pyspark  3.5.1     filter  4.0251     0.2942  2522          11
polars   1.33.0    filter  0.0029     0.0011  2522           1
duckdb   1.3.2     filter  0.0212     0.0010  2522           1
```
