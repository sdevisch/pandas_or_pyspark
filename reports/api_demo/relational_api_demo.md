# Relational API demos

Generated at: 2025-09-21 12:27:23

Generated at: 2025-09-21 12:27:23
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11

```text
backend  version   op      load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2     join    0.1701     0.0566  2000000           1
pandas   2.2.2     concat  0.0000     0.0301  2000000           1
dask     2024.5.1  join    0.0049     0.2340  2000000          11
dask     2024.5.1  concat  0.0000     0.2006  1000000          11
pyspark  3.5.1     join    4.2331     6.5454  2000000          11
pyspark  3.5.1     concat  0.0000     5.1680  2000000          11
polars   1.33.0    join    0.0212     0.0575  2000000           1
polars   1.33.0    concat  0.0000     0.0312  2000000           1
duckdb   1.3.2     join    0.1155     0.0510  2000000           1
duckdb   1.3.2     concat  0.0000     0.0296  2000000           1
```
