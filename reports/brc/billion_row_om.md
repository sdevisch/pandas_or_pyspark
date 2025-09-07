# Billion Row OM Runner

Generated at: 2025-09-07 10:11:40

```text
backend  rows(sci)  operation  source   read_s  compute_s  input_rows   ok  sanity_ok
-------  ---------  ---------  -------  ------  ---------  ----------  ---  ---------
pandas   1.0e+02    groupby    parquet  0.0266     0.0007         100  yes        yes
pandas   1.0e+03    groupby    parquet  0.0261     0.0008        1000  yes        yes
pandas   1.0e+04    groupby    parquet  0.0259     0.0009       10000  yes        yes
pandas   1.0e+05    groupby    parquet  0.0276     0.0027      100000  yes        yes
pandas   1.0e+06    groupby    parquet  0.0382     0.0203     1000000  yes        yes
pandas   1.0e+07    groupby    parquet  0.1279     0.1997    10000000  yes        yes
pandas   1.0e+08    groupby    parquet       -          -           -   no        yes
dask     1.0e+02    groupby    parquet  0.0039     0.0112         100  yes        yes
dask     1.0e+03    groupby    parquet  0.0031     0.0114        1000  yes        yes
dask     1.0e+04    groupby    parquet  0.0033     0.0118       10000  yes        yes
dask     1.0e+05    groupby    parquet  0.0034     0.0136      100000  yes        yes
dask     1.0e+06    groupby    parquet  0.0032     0.0384     1000000  yes        yes
dask     1.0e+07    groupby    parquet  0.0031     0.2818    10000000  yes        yes
dask     1.0e+08    groupby    parquet  0.0180     1.3273   190000000  yes        yes
pyspark  1.0e+02    groupby    parquet  4.0166     1.2176         100  yes        yes
pyspark  1.0e+03    groupby    parquet  3.1819     1.6533        1000  yes        yes
pyspark  1.0e+04    groupby    parquet  3.3217     1.1566       10000  yes        yes
pyspark  1.0e+05    groupby    parquet  3.1416     1.1901      100000  yes        yes
pyspark  1.0e+06    groupby    parquet  3.1696     1.2482     1000000  yes        yes
pyspark  1.0e+07    groupby    parquet  3.1822     1.4425    10000000  yes        yes
pyspark  1.0e+08    groupby    parquet       -          -           -   no        yes
polars   1.0e+02    groupby    parquet  0.3574     0.0008         100  yes        yes
polars   1.0e+03    groupby    parquet  0.3403     0.0008        1000  yes        yes
polars   1.0e+04    groupby    parquet  0.3131     0.0009       10000  yes        yes
polars   1.0e+05    groupby    parquet  0.3207     0.0027      100000  yes        yes
polars   1.0e+06    groupby    parquet  0.3307     0.0200     1000000  yes        yes
polars   1.0e+07    groupby    parquet  0.4578     0.1993    10000000  yes        yes
polars   1.0e+08    groupby    parquet       -          -           -   no        yes
duckdb   1.0e+02    groupby    parquet  0.3524     0.0008         100  yes        yes
duckdb   1.0e+03    groupby    parquet  0.3153     0.0011        1000  yes        yes
duckdb   1.0e+04    groupby    parquet  0.3189     0.0010       10000  yes        yes
duckdb   1.0e+05    groupby    parquet  0.3337     0.0027      100000  yes        yes
duckdb   1.0e+06    groupby    parquet  0.3398     0.0204     1000000  yes        yes
duckdb   1.0e+07    groupby    parquet  0.5117     0.1902    10000000  yes        yes
duckdb   1.0e+08    groupby    parquet       -          -           -   no        yes
```
