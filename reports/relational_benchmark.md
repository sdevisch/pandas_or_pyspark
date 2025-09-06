# Relational benchmarks

Generated at: 2025-09-06 12:11:21

```text
backend  version       op  load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2       join  0.1703     0.0626  2000000           1
pandas   2.2.2     concat  0.0000     0.0301  2000000           1
dask     2024.5.1    join  0.0052     0.2371  2000000          11
dask     2024.5.1  concat  0.0000     0.1934  1000000          11
pyspark  3.5.1       join  4.3378     6.4027  2000000          11
pyspark  3.5.1     concat  0.0000     5.3148  2000000          11
polars   1.33.0      join  0.0213     0.0534  2000000           1
polars   1.33.0    concat  0.0000     0.0302  2000000           1
duckdb   1.3.2       join  0.1138     0.0497  2000000           1
duckdb   1.3.2     concat  0.0000     0.0298  2000000           1
```
