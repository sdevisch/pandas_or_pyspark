# Relational API demos

Generated at: 2025-09-08 16:30:49

```text
backend  version       op  load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2       join  0.1766     0.0573  2000000           1
pandas   2.2.2     concat  0.0000     0.0307  2000000           1
dask     2024.5.1    join  0.0049     0.2384  2000000          11
dask     2024.5.1  concat  0.0000     0.2010  1000000          11
pyspark  3.5.1       join  4.3432     6.3207  2000000          11
pyspark  3.5.1     concat  0.0000     5.3065  2000000          11
polars   1.33.0      join  0.0242     0.0549  2000000           1
polars   1.33.0    concat  0.0000     0.0311  2000000           1
duckdb   1.3.2       join  0.1143     0.0487  2000000           1
duckdb   1.3.2     concat  0.0000     0.0282  2000000           1
```
