# Relational API demos

Generated at: 2025-09-21 14:36:54

Generated at: 2025-09-21 14:36:54
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11

```text
backend  version   op      load_s  compute_s     rows  used_cores
-------  --------  ------  ------  ---------  -------  ----------
pandas   2.2.2     join    0.1764     0.0575  2000000           1
pandas   2.2.2     concat  0.0000     0.0316  2000000           1
dask     2024.5.1  join    0.0046     0.2354  2000000          11
dask     2024.5.1  concat  0.0000     0.1988  1000000          11
pyspark  3.5.1     join    4.2096     6.5637  2000000          11
pyspark  3.5.1     concat  0.0000     5.3076  2000000          11
polars   1.33.0    join    0.0213     0.0567  2000000           1
polars   1.33.0    concat  0.0000     0.0318  2000000           1
duckdb   1.3.2     join    0.1154     0.0517  2000000           1
duckdb   1.3.2     concat  0.0000     0.0305  2000000           1
```
