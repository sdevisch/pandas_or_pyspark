# Relational API demos

Generated at: 2025-09-07 16:22:52

```text
backend  version       op  load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2       join  0.1754     0.0609  2000000           1
pandas   2.2.2     concat  0.0000     0.0346  2000000           1
dask     2024.5.1    join  0.0056     0.2302  2000000          11
dask     2024.5.1  concat  0.0000     0.1906  1000000          11
pyspark  3.5.1       join  4.4783     6.4850  2000000          11
pyspark  3.5.1     concat  0.0000     5.3562  2000000          11
polars   1.33.0      join  0.0241     0.0598  2000000           1
polars   1.33.0    concat  0.0000     0.0304  2000000           1
duckdb   1.3.2       join  0.1171     0.0525  2000000           1
duckdb   1.3.2     concat  0.0000     0.0334  2000000           1
```
