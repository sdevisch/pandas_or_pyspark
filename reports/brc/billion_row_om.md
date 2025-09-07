# Billion Row OM Runner

Generated at: 2025-09-07 16:43:33

```text
backend  rows(sci)  operation  source   read_s  compute_s  input_rows   ok  sanity_ok
-------  ---------  ---------  -------  ------  ---------  ----------  ---  ---------
pandas   1.0e+02    groupby    parquet  0.3517     0.0008         100  yes        yes
pandas   1.0e+03    groupby    parquet  0.0009     0.0006        1000  yes        yes
pandas   1.0e+04    groupby    parquet  0.0012     0.0007       10000  yes        yes
pandas   1.0e+05    groupby    parquet  0.0027     0.0024      100000  yes        yes
pandas   1.0e+06    groupby    parquet  0.0138     0.0200     1000000  yes        yes
pandas   1.0e+07    groupby    parquet  0.1043     0.1985    10000000  yes        yes
pandas   1.0e+08    groupby    parquet  5.8940     4.5160   190000000  yes        yes
pandas   1.0e+09    groupby    parquet       -          -           -   no        yes
dask     1.0e+02    groupby    parquet  0.4128     0.0261         100  yes        yes
dask     1.0e+03    groupby    parquet  0.0019     0.0179        1000  yes        yes
dask     1.0e+04    groupby    parquet  0.0014     0.0166       10000  yes        yes
dask     1.0e+05    groupby    parquet  0.0014     0.0204      100000  yes        yes
dask     1.0e+06    groupby    parquet  0.0019     0.0446     1000000  yes        yes
dask     1.0e+07    groupby    parquet  0.0018     0.3069    10000000  yes        yes
dask     1.0e+08    groupby    parquet  0.0225     1.6960   190000000  yes        yes
dask     1.0e+09    groupby    parquet  0.2518    17.5551  1000000000  yes        yes
pyspark  1.0e+02    groupby    parquet  3.9694     1.1431         100  yes        yes
pyspark  1.0e+03    groupby    parquet  3.1765     1.0937        1000  yes        yes
pyspark  1.0e+04    groupby    parquet  3.1435     1.1466       10000  yes        yes
pyspark  1.0e+05    groupby    parquet  3.3527     1.1636      100000  yes        yes
pyspark  1.0e+06    groupby    parquet  3.1748     1.1815     1000000  yes        yes
pyspark  1.0e+07    groupby    parquet  3.1724     1.3386    10000000  yes        yes
pyspark  1.0e+08    groupby    parquet  3.9003     4.5092   190000000  yes        yes
pyspark  1.0e+09    groupby    parquet       -          -           -   no        yes
polars   1.0e+02    groupby    parquet  0.1473     0.0009         100  yes        yes
polars   1.0e+03    groupby    parquet  0.0038     0.0006        1000  yes        yes
polars   1.0e+04    groupby    parquet  0.0035     0.0009       10000  yes        yes
polars   1.0e+05    groupby    parquet  0.0052     0.0038      100000  yes        yes
polars   1.0e+06    groupby    parquet  0.0615     0.0199     1000000  yes        yes
polars   1.0e+07    groupby    parquet  0.1407     0.1932    10000000  yes        yes
polars   1.0e+08    groupby    parquet  6.6716     4.4297   190000000  yes        yes
polars   1.0e+09    groupby    parquet       -          -           -   no        yes
duckdb   1.0e+02    groupby    parquet  0.3091     0.0008         100  yes        yes
duckdb   1.0e+03    groupby    parquet  0.0072     0.0007        1000  yes        yes
duckdb   1.0e+04    groupby    parquet  0.0086     0.0009       10000  yes        yes
duckdb   1.0e+05    groupby    parquet  0.0122     0.0034      100000  yes        yes
duckdb   1.0e+06    groupby    parquet  0.0408     0.0206     1000000  yes        yes
duckdb   1.0e+07    groupby    parquet  0.1941     0.2002    10000000  yes        yes
duckdb   1.0e+08    groupby    parquet  7.5014     4.1912   190000000  yes        yes
duckdb   1.0e+09    groupby    parquet       -          -           -   no        yes
```
