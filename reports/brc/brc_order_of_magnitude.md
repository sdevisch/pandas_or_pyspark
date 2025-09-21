# Billion Row OM Runner

Generated at: 2025-09-21 12:24:56

```text
backend  rows(sci)  operation  source   read_s  compute_s  input_rows   ok  sanity_ok
-------  ---------  ---------  -------  ------  ---------  ----------  ---  ---------
pandas   1.0e+02    groupby    parquet  0.3277     0.0008         100  yes        yes
pandas   1.0e+03    groupby    parquet  0.0010     0.0005        1000  yes        yes
pandas   1.0e+04    groupby    parquet  0.0010     0.0007       10000  yes        yes
pandas   1.0e+05    groupby    parquet  0.0022     0.0024      100000  yes        yes
pandas   1.0e+06    groupby    parquet  0.0139     0.0202     1000000  yes        yes
pandas   1.0e+07    groupby    parquet  0.1032     0.1993    10000000  yes        yes
pandas   1.0e+08    groupby    parquet       -          -           -   no        yes
dask     1.0e+02    groupby    parquet  0.2411     0.0242         100  yes        yes
dask     1.0e+03    groupby    parquet  0.0013     0.0149        1000  yes        yes
dask     1.0e+04    groupby    parquet  0.0012     0.0151       10000  yes        yes
dask     1.0e+05    groupby    parquet  0.0013     0.0183      100000  yes        yes
dask     1.0e+06    groupby    parquet  0.0012     0.0418     1000000  yes        yes
dask     1.0e+07    groupby    parquet  0.0013     0.2913    10000000  yes        yes
dask     1.0e+08    groupby    parquet  0.0175     1.2810   190000000  yes        yes
dask     1.0e+09    groupby    parquet       -          -           -   no        yes
pyspark  1.0e+02    groupby    parquet       -          -           -   no        yes
polars   1.0e+02    groupby    parquet  0.0926     0.0009         100  yes        yes
polars   1.0e+03    groupby    parquet  0.0037     0.0007        1000  yes        yes
polars   1.0e+04    groupby    parquet  0.0038     0.0007       10000  yes        yes
polars   1.0e+05    groupby    parquet  0.0041     0.0027      100000  yes        yes
polars   1.0e+06    groupby    parquet  0.0359     0.0214     1000000  yes        yes
polars   1.0e+07    groupby    parquet  0.1554     0.2022    10000000  yes        yes
polars   1.0e+08    groupby    parquet       -          -           -   no        yes
duckdb   1.0e+02    groupby    parquet  0.0744     0.0007         100  yes        yes
duckdb   1.0e+03    groupby    parquet  0.0064     0.0006        1000  yes        yes
duckdb   1.0e+04    groupby    parquet  0.0062     0.0009       10000  yes        yes
duckdb   1.0e+05    groupby    parquet  0.0092     0.0034      100000  yes        yes
duckdb   1.0e+06    groupby    parquet  0.0376     0.0208     1000000  yes        yes
duckdb   1.0e+07    groupby    parquet  0.1799     0.1904    10000000  yes        yes
duckdb   1.0e+08    groupby    parquet       -          -           -   no        yes
numpy    1.0e+02    groupby    parquet  0.0127     0.0006         100  yes        yes
numpy    1.0e+03    groupby    parquet  0.0012     0.0006        1000  yes        yes
numpy    1.0e+04    groupby    parquet  0.0029     0.0014       10000  yes        yes
numpy    1.0e+05    groupby    parquet  0.0060     0.0035      100000  yes        yes
numpy    1.0e+06    groupby    parquet  0.0412     0.0200     1000000  yes        yes
numpy    1.0e+07    groupby    parquet  0.1890     0.1910    10000000  yes        yes
numpy    1.0e+08    groupby    parquet       -          -           -   no        yes
numba    1.0e+02    groupby    parquet  0.0027     0.0006         100  yes        yes
numba    1.0e+03    groupby    parquet  0.0010     0.0006        1000  yes        yes
numba    1.0e+04    groupby    parquet  0.0011     0.0007       10000  yes        yes
numba    1.0e+05    groupby    parquet  0.0032     0.0025      100000  yes        yes
numba    1.0e+06    groupby    parquet  0.0146     0.0202     1000000  yes        yes
numba    1.0e+07    groupby    parquet  0.1100     0.1913    10000000  yes        yes
numba    1.0e+08    groupby    parquet       -          -           -   no        yes
```
