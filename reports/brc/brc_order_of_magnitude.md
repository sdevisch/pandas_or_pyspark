# Billion Row OM Runner

Generated at: 2025-09-20 09:42:07

```text
backend  rows(sci)  operation  source   read_s  compute_s  input_rows   ok  sanity_ok
-------  ---------  ---------  -------  ------  ---------  ----------  ---  ---------
pandas   1.0e+02    groupby    parquet  0.3449     0.0007         100  yes        yes
pandas   1.0e+03    groupby    parquet  0.0012     0.0006        1000  yes        yes
pandas   1.0e+04    groupby    parquet  0.0014     0.0007       10000  yes        yes
pandas   1.0e+05    groupby    parquet  0.0028     0.0024      100000  yes        yes
pandas   1.0e+06    groupby    parquet  0.0150     0.0201     1000000  yes        yes
pandas   1.0e+07    groupby    parquet  0.1085     0.1917    10000000  yes        yes
pandas   1.0e+08    groupby    parquet       -          -           -   no        yes
dask     1.0e+02    groupby    parquet  0.1686     0.0167         100  yes        yes
dask     1.0e+03    groupby    parquet  0.0013     0.0158        1000  yes        yes
dask     1.0e+04    groupby    parquet  0.0011     0.0154       10000  yes        yes
dask     1.0e+05    groupby    parquet  0.0012     0.0180      100000  yes        yes
dask     1.0e+06    groupby    parquet  0.0013     0.0411     1000000  yes        yes
dask     1.0e+07    groupby    parquet  0.0015     0.2868    10000000  yes        yes
dask     1.0e+08    groupby    parquet  0.0154     1.2992   190000000  yes        yes
dask     1.0e+09    groupby    parquet       -          -           -   no        yes
pyspark  1.0e+02    groupby    parquet       -          -           -   no        yes
polars   1.0e+02    groupby    parquet  0.0962     0.0010         100  yes        yes
polars   1.0e+03    groupby    parquet  0.0050     0.0006        1000  yes        yes
polars   1.0e+04    groupby    parquet  0.0033     0.0009       10000  yes        yes
polars   1.0e+05    groupby    parquet  0.0049     0.0030      100000  yes        yes
polars   1.0e+06    groupby    parquet  0.0353     0.0207     1000000  yes        yes
polars   1.0e+07    groupby    parquet  0.1541     0.2043    10000000  yes        yes
polars   1.0e+08    groupby    parquet       -          -           -   no        yes
duckdb   1.0e+02    groupby    parquet  0.0848     0.0007         100  yes        yes
duckdb   1.0e+03    groupby    parquet  0.0061     0.0006        1000  yes        yes
duckdb   1.0e+04    groupby    parquet  0.0068     0.0008       10000  yes        yes
duckdb   1.0e+05    groupby    parquet  0.0099     0.0027      100000  yes        yes
duckdb   1.0e+06    groupby    parquet  0.0381     0.0203     1000000  yes        yes
duckdb   1.0e+07    groupby    parquet  0.1918     0.1912    10000000  yes        yes
duckdb   1.0e+08    groupby    parquet       -          -           -   no        yes
numpy    1.0e+02    groupby    parquet  0.0124     0.0006         100  yes        yes
numpy    1.0e+03    groupby    parquet  0.0017     0.0006        1000  yes        yes
numpy    1.0e+04    groupby    parquet  0.0026     0.0008       10000  yes        yes
numpy    1.0e+05    groupby    parquet  0.0067     0.0028      100000  yes        yes
numpy    1.0e+06    groupby    parquet  0.0404     0.0202     1000000  yes        yes
numpy    1.0e+07    groupby    parquet  0.1940     0.1916    10000000  yes        yes
numpy    1.0e+08    groupby    parquet       -          -           -   no        yes
numba    1.0e+02    groupby    parquet  0.0030     0.0008         100  yes        yes
numba    1.0e+03    groupby    parquet  0.0011     0.0005        1000  yes        yes
numba    1.0e+04    groupby    parquet  0.0022     0.0007       10000  yes        yes
numba    1.0e+05    groupby    parquet  0.0031     0.0025      100000  yes        yes
numba    1.0e+06    groupby    parquet  0.0152     0.0203     1000000  yes        yes
numba    1.0e+07    groupby    parquet  0.1110     0.1910    10000000  yes        yes
numba    1.0e+08    groupby    parquet       -          -           -   no        yes
```
