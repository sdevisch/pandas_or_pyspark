# Relational API demos

Generated at: 2025-09-07 16:11:54

```text
backend  version       op  load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2       join  0.1760     0.0590  2000000           1
pandas   2.2.2     concat  0.0000     0.0321  2000000           1
dask     2024.5.1    join  0.0048     0.2375  2000000          11
dask     2024.5.1  concat  0.0000     0.1988  1000000          11
pyspark  3.5.1       join  4.3598     6.4220  2000000          11
pyspark  3.5.1     concat  0.0000     5.5514  2000000          11
polars   1.33.0      join  0.0215     0.0569  2000000           1
polars   1.33.0    concat  0.0000     0.0311  2000000           1
duckdb   1.3.2       join  0.1171     0.0503  2000000           1
duckdb   1.3.2     concat  0.0000     0.0290  2000000           1
```
