# Relational benchmarks

Generated at: 2025-09-06 08:09:34

```text
backend  version       op  load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2       join  0.1684     0.0590  2000000           1
pandas   2.2.2     concat  0.0000     0.0314  2000000           1
dask     2024.5.1    join  0.0055     0.2375  2000000          11
dask     2024.5.1  concat  0.0000     0.1981  1000000          11
pyspark  3.5.1       join  4.4058     7.1854  2000000          11
pyspark  3.5.1     concat  0.0000     5.3968  2000000          11
```
