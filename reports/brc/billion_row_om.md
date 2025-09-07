# Billion Row OM Runner

Generated at: 2025-09-07 11:13:07

```text
backend  rows(sci)  operation  source     read_s  compute_s  input_rows   ok  sanity_ok
-------  ---------  ---------  -------  --------  ---------  ----------  ---  ---------
pandas   1.0e+02    groupby    parquet    0.3641     0.0007         100  yes        yes
pandas   1.0e+03    groupby    parquet    0.0010     0.0005        1000  yes        yes
pandas   1.0e+04    groupby    parquet    0.0009     0.0006       10000  yes        yes
pandas   1.0e+05    groupby    parquet    0.0021     0.0024      100000  yes        yes
pandas   1.0e+06    groupby    parquet    0.0136     0.0198     1000000  yes        yes
pandas   1.0e+07    groupby    parquet    0.1023     0.1979    10000000  yes        yes
pandas   1.0e+08    groupby    parquet    5.2379     4.3382   190000000  yes        yes
pandas   1.0e+09    groupby    parquet  121.3102   131.8729  1000000000  yes        yes
dask     1.0e+02    groupby    parquet    0.3202     0.0161         100  yes        yes
dask     1.0e+03    groupby    parquet    0.0014     0.0126        1000  yes        yes
dask     1.0e+04    groupby    parquet    0.0014     0.0151       10000  yes        yes
dask     1.0e+05    groupby    parquet    0.0015     0.0205      100000  yes        yes
dask     1.0e+06    groupby    parquet    0.0016     0.1526     1000000  yes        yes
dask     1.0e+07    groupby    parquet    0.0020     0.3093    10000000  yes        yes
dask     1.0e+08    groupby    parquet    0.0190     1.4700   190000000  yes        yes
dask     1.0e+09    groupby    parquet    0.1467    16.3168  1000000000  yes        yes
pyspark  1.0e+02    groupby    parquet    4.2149     1.2263         100  yes        yes
pyspark  1.0e+03    groupby    parquet    3.3984     1.1547        1000  yes        yes
pyspark  1.0e+04    groupby    parquet    3.1646     1.1936       10000  yes        yes
pyspark  1.0e+05    groupby    parquet    3.1946     1.2293      100000  yes        yes
pyspark  1.0e+06    groupby    parquet    3.3089     1.2607     1000000  yes        yes
pyspark  1.0e+07    groupby    parquet    3.1886     1.5123    10000000  yes        yes
pyspark  1.0e+08    groupby    parquet    3.9869     5.7078   190000000  yes        yes
pyspark  1.0e+09    groupby    parquet    9.6009    27.8061  1000000000  yes        yes
polars   1.0e+02    groupby    parquet    0.1344     0.0013         100  yes        yes
polars   1.0e+03    groupby    parquet    0.0040     0.0007        1000  yes        yes
polars   1.0e+04    groupby    parquet    0.0027     0.0009       10000  yes        yes
polars   1.0e+05    groupby    parquet    0.0051     0.0035      100000  yes        yes
polars   1.0e+06    groupby    parquet    0.0521     0.0227     1000000  yes        yes
polars   1.0e+07    groupby    parquet    0.1656     0.2051    10000000  yes        yes
polars   1.0e+08    groupby    parquet    7.0531     4.3365   190000000  yes        yes
polars   1.0e+09    groupby    parquet  136.7298   143.1054  1000000000  yes        yes
duckdb   1.0e+02    groupby    parquet    0.2593     0.0032         100  yes        yes
duckdb   1.0e+03    groupby    parquet    0.0079     0.0006        1000  yes        yes
duckdb   1.0e+04    groupby    parquet    0.0093     0.0013       10000  yes        yes
duckdb   1.0e+05    groupby    parquet    0.0114     0.0026      100000  yes        yes
duckdb   1.0e+06    groupby    parquet    0.0442     0.0207     1000000  yes        yes
duckdb   1.0e+07    groupby    parquet    0.2011     0.1970    10000000  yes        yes
duckdb   1.0e+08    groupby    parquet    6.3194     4.2758   190000000  yes        yes
duckdb   1.0e+09    groupby    parquet  134.0735   139.4468  1000000000  yes        yes
```
