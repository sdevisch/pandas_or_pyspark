# Relational API demos

Generated at: 2025-09-08 16:21:49

```text
backend  version       op  load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2       join  0.1769     0.0602  2000000           1
pandas   2.2.2     concat  0.0000     0.0321  2000000           1
dask     2024.5.1    join  0.0046     0.2404  2000000          11
dask     2024.5.1  concat  0.0000     0.2017  1000000          11
pyspark  3.5.1       join  4.3958     6.4000  2000000          11
pyspark  3.5.1     concat  0.0000     5.2510  2000000          11
polars   1.33.0      join  0.0238     0.0569  2000000           1
polars   1.33.0    concat  0.0000     0.0318  2000000           1
duckdb   1.3.2       join  0.1152     0.0522  2000000           1
duckdb   1.3.2     concat  0.0000     0.0306  2000000           1
```
