# Relational API demos

Generated at: 2025-09-07 15:48:26

```text
backend  version       op  load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2       join  0.1843     0.0628  2000000           1
pandas   2.2.2     concat  0.0000     0.0336  2000000           1
dask     2024.5.1    join  0.0057     0.2467  2000000          11
dask     2024.5.1  concat  0.0000     0.2052  1000000          11
pyspark  3.5.1       join  4.8954     7.5782  2000000          11
pyspark  3.5.1     concat  0.0000     5.7014  2000000          11
polars   1.33.0      join  0.0355     0.0612  2000000           1
polars   1.33.0    concat  0.0000     0.0315  2000000           1
duckdb   1.3.2       join  0.1542     0.0536  2000000           1
duckdb   1.3.2     concat  0.0000     0.0325  2000000           1
```
