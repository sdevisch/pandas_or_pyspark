# Relational benchmarks

Generated at: 2025-09-06 14:49:58

```text
backend  version       op  load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2       join  0.1756     0.0586  2000000           1
pandas   2.2.2     concat  0.0000     0.0301  2000000           1
dask     2024.5.1    join  0.0048     0.2357  2000000          11
dask     2024.5.1  concat  0.0000     0.1939  1000000          11
pyspark  3.5.1       join  4.4319     6.4763  2000000          11
pyspark  3.5.1     concat  0.0000     5.3959  2000000          11
polars   1.33.0      join  0.0215     0.0545  2000000           1
polars   1.33.0    concat  0.0000     0.0318  2000000           1
duckdb   1.3.2       join  0.1143     0.0498  2000000           1
duckdb   1.3.2     concat  0.0000     0.0288  2000000           1
```
