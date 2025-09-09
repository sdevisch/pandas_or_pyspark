# Relational API demos

Generated at: 2025-09-08 18:21:16

```text
backend  version       op  load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2       join  0.1815     0.0614  2000000           1
pandas   2.2.2     concat  0.0000     0.0337  2000000           1
dask     2024.5.1    join  0.0057     0.2426  2000000          11
dask     2024.5.1  concat  0.0000     0.2039  1000000          11
pyspark  3.5.1       join  4.4680     6.5978  2000000          11
pyspark  3.5.1     concat  0.0000     5.6014  2000000          11
polars   1.33.0      join  0.0246     0.0554  2000000           1
polars   1.33.0    concat  0.0000     0.0310  2000000           1
duckdb   1.3.2       join  0.1172     0.0500  2000000           1
duckdb   1.3.2     concat  0.0000     0.0300  2000000           1
```
