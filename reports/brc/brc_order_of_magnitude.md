# Billion Row OM Runner

Generated at: 2025-09-20 13:09:08

```text
backend  rows(sci)  operation  source   read_s  compute_s  input_rows   ok  sanity_ok
-------  ---------  ---------  -------  ------  ---------  ----------  ---  ---------
pandas   1.0e+02    groupby    parquet  0.3415     0.0007         100  yes        yes
pandas   1.0e+03    groupby    parquet  0.0010     0.0006        1000  yes        yes
pandas   1.0e+04    groupby    parquet  0.0017     0.0006       10000  yes        yes
pandas   1.0e+05    groupby    parquet  0.0024     0.0024      100000  yes        yes
pandas   1.0e+06    groupby    parquet  0.0139     0.0201     1000000  yes        yes
pandas   1.0e+07    groupby    parquet  0.1025     0.2074    10000000  yes        yes
pandas   1.0e+08    groupby    parquet       -          -           -   no        yes
dask     1.0e+02    groupby    parquet  0.2062     0.0165         100  yes        yes
dask     1.0e+03    groupby    parquet  0.0011     0.0150        1000  yes        yes
dask     1.0e+04    groupby    parquet  0.0013     0.0158       10000  yes        yes
dask     1.0e+05    groupby    parquet  0.0011     0.0179      100000  yes        yes
dask     1.0e+06    groupby    parquet  0.0013     0.0419     1000000  yes        yes
dask     1.0e+07    groupby    parquet  0.0013     0.2865    10000000  yes        yes
dask     1.0e+08    groupby    parquet  0.0163     1.3093   190000000  yes        yes
dask     1.0e+09    groupby    parquet       -          -           -   no        yes
pyspark  1.0e+02    groupby    parquet       -          -           -   no        yes
polars   1.0e+02    groupby    parquet  0.0955     0.0011         100  yes        yes
polars   1.0e+03    groupby    parquet  0.0028     0.0006        1000  yes        yes
polars   1.0e+04    groupby    parquet  0.0027     0.0006       10000  yes        yes
polars   1.0e+05    groupby    parquet  0.0057     0.0027      100000  yes        yes
polars   1.0e+06    groupby    parquet  0.0363     0.0202     1000000  yes        yes
polars   1.0e+07    groupby    parquet  0.1517     0.2029    10000000  yes        yes
polars   1.0e+08    groupby    parquet       -          -           -   no        yes
duckdb   1.0e+02    groupby    parquet  0.0361     0.0007         100  yes        yes
duckdb   1.0e+03    groupby    parquet  0.0064     0.0007        1000  yes        yes
duckdb   1.0e+04    groupby    parquet  0.0058     0.0010       10000  yes        yes
duckdb   1.0e+05    groupby    parquet  0.0085     0.0029      100000  yes        yes
duckdb   1.0e+06    groupby    parquet  0.0341     0.0201     1000000  yes        yes
duckdb   1.0e+07    groupby    parquet  0.1826     0.1897    10000000  yes        yes
duckdb   1.0e+08    groupby    parquet       -          -           -   no        yes
numpy    1.0e+02    groupby    parquet  0.0057     0.0007         100  yes        yes
numpy    1.0e+03    groupby    parquet  0.0015     0.0005        1000  yes        yes
numpy    1.0e+04    groupby    parquet  0.0022     0.0011       10000  yes        yes
numpy    1.0e+05    groupby    parquet  0.0043     0.0029      100000  yes        yes
numpy    1.0e+06    groupby    parquet  0.0458     0.0202     1000000  yes        yes
numpy    1.0e+07    groupby    parquet  0.2058     0.2014    10000000  yes        yes
numpy    1.0e+08    groupby    parquet       -          -           -   no        yes
numba    1.0e+02    groupby    parquet  0.0030     0.0007         100  yes        yes
numba    1.0e+03    groupby    parquet  0.0010     0.0006        1000  yes        yes
numba    1.0e+04    groupby    parquet  0.0010     0.0007       10000  yes        yes
numba    1.0e+05    groupby    parquet  0.0037     0.0029      100000  yes        yes
numba    1.0e+06    groupby    parquet  0.0148     0.0200     1000000  yes        yes
numba    1.0e+07    groupby    parquet  0.1073     0.1903    10000000  yes        yes
numba    1.0e+08    groupby    parquet       -          -           -   no        yes
```
