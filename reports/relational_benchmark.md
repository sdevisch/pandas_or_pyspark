# Relational benchmarks

Generated at: 2025-09-06 12:04:23

```text
backend  version       op  load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2       join  0.1712     0.0612  2000000           1
pandas   2.2.2     concat  0.0000     0.0304  2000000           1
dask     2024.5.1    join  0.0047     0.2391  2000000          11
dask     2024.5.1  concat  0.0000     0.1973  1000000          11
pyspark  3.5.1       join  4.3752     6.5039  2000000          11
pyspark  3.5.1     concat  0.0000     5.2513  2000000          11
polars   1.33.0      join  0.0215     0.0559  2000000           1
polars   1.33.0    concat  0.0000     0.0314  2000000           1
duckdb   1.3.2       join  0.1186     0.0513  2000000           1
duckdb   1.3.2     concat  0.0000     0.0302  2000000           1
```
