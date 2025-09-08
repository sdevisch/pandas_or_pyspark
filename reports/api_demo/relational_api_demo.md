# Relational API demos

Generated at: 2025-09-08 17:52:17

```text
backend  version       op  load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2       join  0.1736     0.0569  2000000           1
pandas   2.2.2     concat  0.0000     0.0327  2000000           1
dask     2024.5.1    join  0.0059     0.2434  2000000          11
dask     2024.5.1  concat  0.0000     0.2052  1000000          11
pyspark  3.5.1       join  4.3620     6.4657  2000000          11
pyspark  3.5.1     concat  0.0000     5.3969  2000000          11
polars   1.33.0      join  0.0245     0.0576  2000000           1
polars   1.33.0    concat  0.0000     0.0308  2000000           1
duckdb   1.3.2       join  0.1158     0.0503  2000000           1
duckdb   1.3.2     concat  0.0000     0.0304  2000000           1
```
