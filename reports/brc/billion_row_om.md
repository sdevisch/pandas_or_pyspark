# Billion Row OM Runner

Generated at: 2025-09-07 10:42:42

```text
backend  rows(sci)  operation  source   read_s  compute_s  input_rows   ok  sanity_ok
-------  ---------  ---------  -------  ------  ---------  ----------  ---  ---------
pandas   1.0e+02    groupby    parquet  0.3619     0.0007         100  yes        yes
pandas   1.0e+03    groupby    parquet  0.0008     0.0005        1000  yes        yes
pandas   1.0e+04    groupby    parquet  0.0010     0.0008       10000  yes        yes
pandas   1.0e+05    groupby    parquet  0.0022     0.0022      100000  yes        yes
pandas   1.0e+06    groupby    parquet  0.0137     0.0205     1000000  yes        yes
pandas   1.0e+07    groupby    parquet  0.1038     0.1973    10000000  yes        yes
pandas   1.0e+08    groupby    parquet       -          -           -   no        yes
dask     1.0e+02    groupby    parquet  0.2084     0.0114         100  yes        yes
dask     1.0e+03    groupby    parquet  0.0014     0.0100        1000  yes        yes
dask     1.0e+04    groupby    parquet  0.0014     0.0101       10000  yes        yes
dask     1.0e+05    groupby    parquet  0.0015     0.0129      100000  yes        yes
dask     1.0e+06    groupby    parquet  0.0014     0.0391     1000000  yes        yes
dask     1.0e+07    groupby    parquet  0.0015     0.2864    10000000  yes        yes
dask     1.0e+08    groupby    parquet  0.0170     1.2781   190000000  yes        yes
pyspark  1.0e+02    groupby    parquet  3.5345     1.2153         100  yes        yes
pyspark  1.0e+03    groupby    parquet  3.1479     1.1587        1000  yes        yes
pyspark  1.0e+04    groupby    parquet  3.1369     1.1985       10000  yes        yes
pyspark  1.0e+05    groupby    parquet  3.1081     1.1938      100000  yes        yes
pyspark  1.0e+06    groupby    parquet  3.1317     1.2453     1000000  yes        yes
pyspark  1.0e+07    groupby    parquet  3.1255     1.4888    10000000  yes        yes
pyspark  1.0e+08    groupby    parquet       -          -           -   no        yes
polars   1.0e+02    groupby    parquet  0.1346     0.0007         100  yes        yes
polars   1.0e+03    groupby    parquet  0.0012     0.0006        1000  yes        yes
polars   1.0e+04    groupby    parquet  0.0016     0.0008       10000  yes        yes
polars   1.0e+05    groupby    parquet  0.0031     0.0032      100000  yes        yes
polars   1.0e+06    groupby    parquet  0.0225     0.0224     1000000  yes        yes
polars   1.0e+07    groupby    parquet  0.1435     0.2040    10000000  yes        yes
polars   1.0e+08    groupby    parquet       -          -           -   no        yes
duckdb   1.0e+02    groupby    parquet  0.0898     0.0008         100  yes        yes
duckdb   1.0e+03    groupby    parquet  0.0065     0.0006        1000  yes        yes
duckdb   1.0e+04    groupby    parquet  0.0071     0.0009       10000  yes        yes
duckdb   1.0e+05    groupby    parquet  0.0098     0.0028      100000  yes        yes
duckdb   1.0e+06    groupby    parquet  0.0365     0.0197     1000000  yes        yes
duckdb   1.0e+07    groupby    parquet  0.1865     0.1898    10000000  yes        yes
duckdb   1.0e+08    groupby    parquet       -          -           -   no        yes
```
