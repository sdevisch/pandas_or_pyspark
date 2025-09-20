# Billion Row OM Runner

Generated at: 2025-09-20 13:38:13

```text
backend  rows(sci)  operation  source   read_s  compute_s  input_rows   ok  sanity_ok
-------  ---------  ---------  -------  ------  ---------  ----------  ---  ---------
pandas   1.0e+02    groupby    parquet  0.3349     0.0007         100  yes        yes
pandas   1.0e+03    groupby    parquet  0.0012     0.0005        1000  yes        yes
pandas   1.0e+04    groupby    parquet  0.0014     0.0007       10000  yes        yes
pandas   1.0e+05    groupby    parquet  0.0028     0.0025      100000  yes        yes
pandas   1.0e+06    groupby    parquet  0.0155     0.0203     1000000  yes        yes
pandas   1.0e+07    groupby    parquet  0.1109     0.1983    10000000  yes        yes
pandas   1.0e+08    groupby    parquet       -          -           -   no        yes
dask     1.0e+02    groupby    parquet  0.2217     0.0235         100  yes        yes
dask     1.0e+03    groupby    parquet  0.0017     0.0158        1000  yes        yes
dask     1.0e+04    groupby    parquet  0.0014     0.0156       10000  yes        yes
dask     1.0e+05    groupby    parquet  0.0014     0.0187      100000  yes        yes
dask     1.0e+06    groupby    parquet  0.0014     0.0455     1000000  yes        yes
dask     1.0e+07    groupby    parquet  0.0016     0.2932    10000000  yes        yes
dask     1.0e+08    groupby    parquet  0.0181     1.3266   190000000  yes        yes
dask     1.0e+09    groupby    parquet       -          -           -   no        yes
pyspark  1.0e+02    groupby    parquet       -          -           -   no        yes
polars   1.0e+02    groupby    parquet  0.1843     0.0014         100  yes        yes
polars   1.0e+03    groupby    parquet  0.0080     0.0006        1000  yes        yes
polars   1.0e+04    groupby    parquet  0.0024     0.0007       10000  yes        yes
polars   1.0e+05    groupby    parquet  0.0067     0.0026      100000  yes        yes
polars   1.0e+06    groupby    parquet  0.0459     0.0201     1000000  yes        yes
polars   1.0e+07    groupby    parquet  0.1546     0.2015    10000000  yes        yes
polars   1.0e+08    groupby    parquet       -          -           -   no        yes
duckdb   1.0e+02    groupby    parquet  0.0764     0.0008         100  yes        yes
duckdb   1.0e+03    groupby    parquet  0.0060     0.0006        1000  yes        yes
duckdb   1.0e+04    groupby    parquet  0.0064     0.0007       10000  yes        yes
duckdb   1.0e+05    groupby    parquet  0.0099     0.0028      100000  yes        yes
duckdb   1.0e+06    groupby    parquet  0.0366     0.0201     1000000  yes        yes
duckdb   1.0e+07    groupby    parquet  0.1901     0.1896    10000000  yes        yes
duckdb   1.0e+08    groupby    parquet       -          -           -   no        yes
numpy    1.0e+02    groupby    parquet  0.0122     0.0006         100  yes        yes
numpy    1.0e+03    groupby    parquet  0.0017     0.0005        1000  yes        yes
numpy    1.0e+04    groupby    parquet  0.0025     0.0009       10000  yes        yes
numpy    1.0e+05    groupby    parquet  0.0082     0.0026      100000  yes        yes
numpy    1.0e+06    groupby    parquet  0.0532     0.0201     1000000  yes        yes
numpy    1.0e+07    groupby    parquet  0.2233     0.1954    10000000  yes        yes
numpy    1.0e+08    groupby    parquet       -          -           -   no        yes
numba    1.0e+02    groupby    parquet  0.0035     0.0006         100  yes        yes
numba    1.0e+03    groupby    parquet  0.0013     0.0006        1000  yes        yes
numba    1.0e+04    groupby    parquet  0.0015     0.0007       10000  yes        yes
numba    1.0e+05    groupby    parquet  0.0029     0.0025      100000  yes        yes
numba    1.0e+06    groupby    parquet  0.0175     0.0205     1000000  yes        yes
numba    1.0e+07    groupby    parquet  0.1197     0.1911    10000000  yes        yes
numba    1.0e+08    groupby    parquet       -          -           -   no        yes
```
