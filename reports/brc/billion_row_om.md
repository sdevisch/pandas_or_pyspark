# Billion Row OM Runner

Generated at: 2025-09-07 15:55:57

```text
backend  rows(sci)  operation  source   read_s  compute_s  input_rows   ok  sanity_ok
-------  ---------  ---------  -------  ------  ---------  ----------  ---  ---------
pandas   1.0e+02    groupby    parquet  0.3392     0.0008         100  yes        yes
pandas   1.0e+03    groupby    parquet  0.0011     0.0005        1000  yes        yes
pandas   1.0e+04    groupby    parquet  0.0015     0.0008       10000  yes        yes
pandas   1.0e+05    groupby    parquet  0.0029     0.0025      100000  yes        yes
pandas   1.0e+06    groupby    parquet  0.0153     0.0218     1000000  yes        yes
pandas   1.0e+07    groupby    parquet  0.1125     0.2005    10000000  yes        yes
pandas   1.0e+08    groupby    parquet       -          -           -   no        yes
dask     1.0e+02    groupby    parquet  0.2402     0.0180         100  yes        yes
dask     1.0e+03    groupby    parquet  0.0013     0.0148        1000  yes        yes
dask     1.0e+04    groupby    parquet  0.0017     0.0159       10000  yes        yes
dask     1.0e+05    groupby    parquet  0.0017     0.0186      100000  yes        yes
dask     1.0e+06    groupby    parquet  0.0015     0.0456     1000000  yes        yes
dask     1.0e+07    groupby    parquet  0.0018     0.2959    10000000  yes        yes
dask     1.0e+08    groupby    parquet  0.0180     1.3599   190000000  yes        yes
dask     1.0e+09    groupby    parquet       -          -           -   no        yes
pyspark  1.0e+02    groupby    parquet  4.0346     1.1180         100  yes        yes
pyspark  1.0e+03    groupby    parquet  3.1923     1.1577        1000  yes        yes
pyspark  1.0e+04    groupby    parquet  3.1959     1.1488       10000  yes        yes
pyspark  1.0e+05    groupby    parquet  3.1549     1.1570      100000  yes        yes
pyspark  1.0e+06    groupby    parquet  3.1588     1.1503     1000000  yes        yes
pyspark  1.0e+07    groupby    parquet  3.1645     1.3538    10000000  yes        yes
pyspark  1.0e+08    groupby    parquet       -          -           -   no        yes
polars   1.0e+02    groupby    parquet  0.1403     0.0013         100  yes        yes
polars   1.0e+03    groupby    parquet  0.0032     0.0005        1000  yes        yes
polars   1.0e+04    groupby    parquet  0.0022     0.0007       10000  yes        yes
polars   1.0e+05    groupby    parquet  0.0038     0.0030      100000  yes        yes
polars   1.0e+06    groupby    parquet  0.0541     0.0218     1000000  yes        yes
polars   1.0e+07    groupby    parquet  0.1591     0.2124    10000000  yes        yes
polars   1.0e+08    groupby    parquet       -          -           -   no        yes
duckdb   1.0e+02    groupby    parquet  0.0886     0.0008         100  yes        yes
duckdb   1.0e+03    groupby    parquet  0.0065     0.0007        1000  yes        yes
duckdb   1.0e+04    groupby    parquet  0.0069     0.0008       10000  yes        yes
duckdb   1.0e+05    groupby    parquet  0.0097     0.0025      100000  yes        yes
duckdb   1.0e+06    groupby    parquet  0.0367     0.0199     1000000  yes        yes
duckdb   1.0e+07    groupby    parquet  0.1870     0.1913    10000000  yes        yes
duckdb   1.0e+08    groupby    parquet       -          -           -   no        yes
```
