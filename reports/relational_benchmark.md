# Relational benchmarks

Generated at: 2025-09-06 09:42:25

```text
backend  version       op  load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2       join  0.1765     0.0589  2000000           1
pandas   2.2.2     concat  0.0000     0.0322  2000000           1
dask     2024.5.1    join  0.0053     0.2360  2000000          11
dask     2024.5.1  concat  0.0000     0.1984  1000000          11
pyspark  3.5.1       join  4.3866     7.2797  2000000          11
pyspark  3.5.1     concat  0.0000     5.6238  2000000          11
```
