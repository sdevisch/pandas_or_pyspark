# Relational API demos

Generated at: 2025-09-08 16:45:49

```text
backend  version       op  load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2       join  0.2482     0.0570  2000000           1
pandas   2.2.2     concat  0.0000     0.0314  2000000           1
dask     2024.5.1    join  0.0052     0.2288  2000000          11
dask     2024.5.1  concat  0.0000     0.1961  1000000          11
pyspark  3.5.1       join  4.3042     6.5151  2000000          11
pyspark  3.5.1     concat  0.0000     5.2438  2000000          11
polars   1.33.0      join  0.0241     0.0554  2000000           1
polars   1.33.0    concat  0.0000     0.0298  2000000           1
duckdb   1.3.2       join  0.1140     0.0506  2000000           1
duckdb   1.3.2     concat  0.0000     0.0288  2000000           1
```
