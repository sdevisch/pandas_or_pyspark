# Relational API demos

Generated at: 2025-09-21 14:39:15

Generated at: 2025-09-21 14:39:15
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11

```text
backend  version   op      load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2     join    0.1755     0.0613  2000000           1
pandas   2.2.2     concat  0.0000     0.0320  2000000           1
dask     2024.5.1  join    0.0048     0.2400  2000000          11
dask     2024.5.1  concat  0.0000     0.2006  1000000          11
pyspark  3.5.1     join    4.5667     6.7661  2000000          11
pyspark  3.5.1     concat  0.0000     5.2984  2000000          11
polars   1.33.0    join    0.0219     0.0564  2000000           1
polars   1.33.0    concat  0.0000     0.0316  2000000           1
duckdb   1.3.2     join    0.1143     0.0505  2000000           1
duckdb   1.3.2     concat  0.0000     0.0305  2000000           1
```
