# Relational API demos

Generated at: 2025-09-21 14:43:01

Generated at: 2025-09-21 14:43:01
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11

```text
backend  version   op      load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2     join    0.1718     0.0550  2000000           1
pandas   2.2.2     concat  0.0000     0.0291  2000000           1
dask     2024.5.1  join    0.0048     0.2351  2000000          11
dask     2024.5.1  concat  0.0000     0.2005  1000000          11
pyspark  3.5.1     join    4.3935     6.3719  2000000          11
pyspark  3.5.1     concat  0.0000     5.3478  2000000          11
polars   1.33.0    join    0.0209     0.0552  2000000           1
polars   1.33.0    concat  0.0000     0.0309  2000000           1
duckdb   1.3.2     join    0.1192     0.0497  2000000           1
duckdb   1.3.2     concat  0.0000     0.0293  2000000           1
```
