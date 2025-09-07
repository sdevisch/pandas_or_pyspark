# Relational benchmarks

Generated at: 2025-09-07 09:23:58

```text
backend  version       op  load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2       join  0.2706     0.0552  2000000           1
pandas   2.2.2     concat  0.0000     0.0302  2000000           1
dask     2024.5.1    join  0.0053     0.2375  2000000          11
dask     2024.5.1  concat  0.0000     0.1930  1000000          11
pyspark  3.5.1       join  4.3869     6.6332  2000000          11
pyspark  3.5.1     concat  0.0000     5.1814  2000000          11
polars   1.33.0      join  0.0212     0.0550  2000000           1
polars   1.33.0    concat  0.0000     0.0320  2000000           1
duckdb   1.3.2       join  0.1146     0.0494  2000000           1
duckdb   1.3.2     concat  0.0000     0.0294  2000000           1
```
