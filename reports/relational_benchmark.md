# Relational benchmarks

Generated at: 2025-09-06 16:46:53

```text
backend  version       op  load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2       join  0.1743     0.0583  2000000           1
pandas   2.2.2     concat  0.0000     0.0312  2000000           1
dask     2024.5.1    join  0.0048     0.2446  2000000          11
dask     2024.5.1  concat  0.0000     0.1943  1000000          11
pyspark  3.5.1       join  4.4417     6.6610  2000000          11
pyspark  3.5.1     concat  0.0000     5.3527  2000000          11
polars   1.33.0      join  0.0220     0.0569  2000000           1
polars   1.33.0    concat  0.0000     0.0305  2000000           1
duckdb   1.3.2       join  0.1135     0.0509  2000000           1
duckdb   1.3.2     concat  0.0000     0.0322  2000000           1
```
