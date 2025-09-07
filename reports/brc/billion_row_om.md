# Billion Row OM Runner

Generated at: 2025-09-07 16:17:32

```text
backend  rows(sci)  operation  source   read_s  compute_s  input_rows   ok  sanity_ok
-------  ---------  ---------  -------  ------  ---------  ----------  ---  ---------
pandas   1.0e+02    groupby    parquet  0.2997     0.0007         100  yes        yes
pandas   1.0e+03    groupby    parquet  0.0009     0.0006        1000  yes        yes
pandas   1.0e+04    groupby    parquet  0.0017     0.0007       10000  yes        yes
pandas   1.0e+05    groupby    parquet  0.0026     0.0024      100000  yes        yes
pandas   1.0e+06    groupby    parquet  0.0146     0.0199     1000000  yes        yes
pandas   1.0e+07    groupby    parquet  0.1023     0.1907    10000000  yes        yes
pandas   1.0e+08    groupby    parquet       -          -           -   no        yes
dask     1.0e+02    groupby    parquet  0.2152     0.0164         100  yes        yes
dask     1.0e+03    groupby    parquet  0.0013     0.0143        1000  yes        yes
dask     1.0e+04    groupby    parquet  0.0013     0.0144       10000  yes        yes
dask     1.0e+05    groupby    parquet  0.0013     0.0169      100000  yes        yes
dask     1.0e+06    groupby    parquet  0.0014     0.0429     1000000  yes        yes
dask     1.0e+07    groupby    parquet  0.0015     0.2850    10000000  yes        yes
dask     1.0e+08    groupby    parquet  0.0167     1.2626   190000000  yes        yes
dask     1.0e+09    groupby    parquet       -          -           -   no        yes
pyspark  1.0e+02    groupby    parquet  3.9632     1.3026         100  yes        yes
pyspark  1.0e+03    groupby    parquet  3.4538     1.1777        1000  yes        yes
pyspark  1.0e+04    groupby    parquet  3.1636     1.2610       10000  yes        yes
pyspark  1.0e+05    groupby    parquet  3.1782     1.1519      100000  yes        yes
pyspark  1.0e+06    groupby    parquet  3.6441     1.2531     1000000  yes        yes
pyspark  1.0e+07    groupby    parquet  3.1573     1.4301    10000000  yes        yes
pyspark  1.0e+08    groupby    parquet       -          -           -   no        yes
polars   1.0e+02    groupby    parquet  0.1414     0.0008         100  yes        yes
polars   1.0e+03    groupby    parquet  0.0035     0.0005        1000  yes        yes
polars   1.0e+04    groupby    parquet  0.0031     0.0008       10000  yes        yes
polars   1.0e+05    groupby    parquet  0.0058     0.0028      100000  yes        yes
polars   1.0e+06    groupby    parquet  0.0366     0.0207     1000000  yes        yes
polars   1.0e+07    groupby    parquet  0.1588     0.2073    10000000  yes        yes
polars   1.0e+08    groupby    parquet       -          -           -   no        yes
duckdb   1.0e+02    groupby    parquet  0.0916     0.0009         100  yes        yes
duckdb   1.0e+03    groupby    parquet  0.0063     0.0007        1000  yes        yes
duckdb   1.0e+04    groupby    parquet  0.0069     0.0009       10000  yes        yes
duckdb   1.0e+05    groupby    parquet  0.0097     0.0028      100000  yes        yes
duckdb   1.0e+06    groupby    parquet  0.0363     0.0203     1000000  yes        yes
duckdb   1.0e+07    groupby    parquet  0.2058     0.2454    10000000  yes        yes
duckdb   1.0e+08    groupby    parquet       -          -           -   no        yes
```
