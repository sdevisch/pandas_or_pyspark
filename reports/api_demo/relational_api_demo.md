# Relational API demos

Generated at: 2025-09-21 14:11:15

Generated at: 2025-09-21 14:11:15
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11

```text
backend  version   op      load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2     join    0.1761     0.0578  2000000           1
pandas   2.2.2     concat  0.0000     0.0313  2000000           1
dask     2024.5.1  join    0.0048     0.2327  2000000          11
dask     2024.5.1  concat  0.0000     0.1987  1000000          11
pyspark  3.5.1     join    4.3375     6.6098  2000000          11
pyspark  3.5.1     concat  0.0000     5.3303  2000000          11
polars   1.33.0    join    0.0219     0.0565  2000000           1
polars   1.33.0    concat  0.0000     0.0312  2000000           1
duckdb   1.3.2     join    0.1141     0.0511  2000000           1
duckdb   1.3.2     concat  0.0000     0.0297  2000000           1
```
