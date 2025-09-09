# Billion Row OM Runner

Generated at: 2025-09-08 19:07:27

```text
backend  rows(sci)  operation  source   read_s  compute_s  input_rows   ok  sanity_ok
-------  ---------  ---------  -------  ------  ---------  ----------  ---  ---------
pandas   1.0e+02    groupby    parquet  0.3546     0.0006         100  yes        yes
pandas   1.0e+03    groupby    parquet  0.0010     0.0006        1000  yes        yes
pandas   1.0e+04    groupby    parquet  0.0013     0.0009       10000  yes        yes
pandas   1.0e+05    groupby    parquet  0.0033     0.0024      100000  yes        yes
pandas   1.0e+06    groupby    parquet  0.0155     0.0224     1000000  yes        yes
pandas   1.0e+07    groupby    parquet  0.1124     0.2017    10000000  yes        yes
pandas   1.0e+08    groupby    parquet  6.2932     4.5568   190000000  yes        yes
pandas   1.0e+09    groupby    parquet       -          -           -   no        yes
dask     1.0e+02    groupby    parquet  0.3317     0.0321         100  yes        yes
dask     1.0e+03    groupby    parquet  0.0020     0.0175        1000  yes        yes
dask     1.0e+04    groupby    parquet  0.0015     0.0170       10000  yes        yes
dask     1.0e+05    groupby    parquet  0.0015     0.0193      100000  yes        yes
dask     1.0e+06    groupby    parquet  0.0016     0.0444     1000000  yes        yes
dask     1.0e+07    groupby    parquet  0.0018     0.3060    10000000  yes        yes
dask     1.0e+08    groupby    parquet  0.0212     1.5026   190000000  yes        yes
dask     1.0e+09    groupby    parquet  0.1765    19.4586  1000000000  yes        yes
pyspark  1.0e+02    groupby    parquet  3.6043     1.3723         100  yes        yes
pyspark  1.0e+03    groupby    parquet  2.9276     1.1468        1000  yes        yes
pyspark  1.0e+04    groupby    parquet  3.2775     1.1680       10000  yes        yes
pyspark  1.0e+05    groupby    parquet  3.2486     1.1887      100000  yes        yes
pyspark  1.0e+06    groupby    parquet  3.0763     1.1945     1000000  yes        yes
pyspark  1.0e+07    groupby    parquet  3.0011     1.3362    10000000  yes        yes
pyspark  1.0e+08    groupby    parquet  3.8607     4.8469   190000000  yes        yes
pyspark  1.0e+09    groupby    parquet       -          -           -   no        yes
polars   1.0e+02    groupby    parquet  0.1031     0.0017         100  yes        yes
polars   1.0e+03    groupby    parquet  0.0049     0.0014        1000  yes        yes
polars   1.0e+04    groupby    parquet  0.0035     0.0010       10000  yes        yes
polars   1.0e+05    groupby    parquet  0.0058     0.0033      100000  yes        yes
polars   1.0e+06    groupby    parquet  0.0716     0.0232     1000000  yes        yes
polars   1.0e+07    groupby    parquet  0.1480     0.2092    10000000  yes        yes
polars   1.0e+08    groupby    parquet  6.8837     4.6818   190000000  yes        yes
polars   1.0e+09    groupby    parquet       -          -           -   no        yes
duckdb   1.0e+02    groupby    parquet  0.3788     0.0008         100  yes        yes
duckdb   1.0e+03    groupby    parquet  0.0131     0.0007        1000  yes        yes
duckdb   1.0e+04    groupby    parquet  0.0082     0.0010       10000  yes        yes
duckdb   1.0e+05    groupby    parquet  0.0120     0.0026      100000  yes        yes
duckdb   1.0e+06    groupby    parquet  0.0422     0.0208     1000000  yes        yes
duckdb   1.0e+07    groupby    parquet  0.2035     0.2602    10000000  yes        yes
duckdb   1.0e+08    groupby    parquet  6.1617     4.2053   190000000  yes        yes
duckdb   1.0e+09    groupby    parquet       -          -           -   no        yes
```
