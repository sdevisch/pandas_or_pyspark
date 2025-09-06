# Relational benchmarks

Generated at: 2025-09-06 14:11:57

```text
backend  version       op  load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2       join  0.1672     0.0571  2000000           1
pandas   2.2.2     concat  0.0000     0.0319  2000000           1
dask     2024.5.1    join  0.0050     0.2302  2000000          11
dask     2024.5.1  concat  0.0000     0.1956  1000000          11
pyspark  3.5.1       join  4.1706     6.6256  2000000          11
pyspark  3.5.1     concat  0.0000     5.4346  2000000          11
polars   1.33.0      join  0.0253     0.0545  2000000           1
polars   1.33.0    concat  0.0000     0.0318  2000000           1
duckdb   1.3.2       join  0.1162     0.0493  2000000           1
duckdb   1.3.2     concat  0.0000     0.0307  2000000           1
```
