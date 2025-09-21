# Billion Row OM Runner

Generated at: 2025-09-21 11:44:24

```text
backend  rows(sci)  operation  source   read_s  compute_s  input_rows   ok  sanity_ok
-------  ---------  ---------  -------  ------  ---------  ----------  ---  ---------
pandas   1.0e+02    groupby    parquet  0.3528     0.0007         100  yes        yes
pandas   1.0e+03    groupby    parquet  0.0014     0.0006        1000  yes        yes
pandas   1.0e+04    groupby    parquet  0.0014     0.0007       10000  yes        yes
pandas   1.0e+05    groupby    parquet  0.0027     0.0026      100000  yes        yes
pandas   1.0e+06    groupby    parquet  0.0149     0.0202     1000000  yes        yes
pandas   1.0e+07    groupby    parquet  0.1030     0.1924    10000000  yes        yes
pandas   1.0e+08    groupby    parquet       -          -           -   no        yes
dask     1.0e+02    groupby    parquet  0.1687     0.0168         100  yes        yes
dask     1.0e+03    groupby    parquet  0.0012     0.0152        1000  yes        yes
dask     1.0e+04    groupby    parquet  0.0013     0.0160       10000  yes        yes
dask     1.0e+05    groupby    parquet  0.0011     0.0183      100000  yes        yes
dask     1.0e+06    groupby    parquet  0.0012     0.0450     1000000  yes        yes
dask     1.0e+07    groupby    parquet  0.0014     0.2927    10000000  yes        yes
dask     1.0e+08    groupby    parquet  0.0157     1.3217   190000000  yes        yes
dask     1.0e+09    groupby    parquet       -          -           -   no        yes
pyspark  1.0e+02    groupby    parquet       -          -           -   no        yes
polars   1.0e+02    groupby    parquet  0.1005     0.0010         100  yes        yes
polars   1.0e+03    groupby    parquet  0.0039     0.0006        1000  yes        yes
polars   1.0e+04    groupby    parquet  0.0034     0.0010       10000  yes        yes
polars   1.0e+05    groupby    parquet  0.0038     0.0025      100000  yes        yes
polars   1.0e+06    groupby    parquet  0.0366     0.0204     1000000  yes        yes
polars   1.0e+07    groupby    parquet  0.1541     0.2034    10000000  yes        yes
polars   1.0e+08    groupby    parquet       -          -           -   no        yes
duckdb   1.0e+02    groupby    parquet  0.0361     0.0007         100  yes        yes
duckdb   1.0e+03    groupby    parquet  0.0064     0.0007        1000  yes        yes
duckdb   1.0e+04    groupby    parquet  0.0063     0.0008       10000  yes        yes
duckdb   1.0e+05    groupby    parquet  0.0089     0.0024      100000  yes        yes
duckdb   1.0e+06    groupby    parquet  0.0344     0.0204     1000000  yes        yes
duckdb   1.0e+07    groupby    parquet  0.1839     0.1923    10000000  yes        yes
duckdb   1.0e+08    groupby    parquet       -          -           -   no        yes
numpy    1.0e+02    groupby    parquet  0.0106     0.0006         100  yes        yes
numpy    1.0e+03    groupby    parquet  0.0016     0.0005        1000  yes        yes
numpy    1.0e+04    groupby    parquet  0.0022     0.0011       10000  yes        yes
numpy    1.0e+05    groupby    parquet  0.0067     0.0028      100000  yes        yes
numpy    1.0e+06    groupby    parquet  0.0510     0.0201     1000000  yes        yes
numpy    1.0e+07    groupby    parquet  0.1976     0.1998    10000000  yes        yes
numpy    1.0e+08    groupby    parquet       -          -           -   no        yes
numba    1.0e+02    groupby    parquet  0.0028     0.0006         100  yes        yes
numba    1.0e+03    groupby    parquet  0.0010     0.0006        1000  yes        yes
numba    1.0e+04    groupby    parquet  0.0010     0.0007       10000  yes        yes
numba    1.0e+05    groupby    parquet  0.0044     0.0025      100000  yes        yes
numba    1.0e+06    groupby    parquet  0.0146     0.0198     1000000  yes        yes
numba    1.0e+07    groupby    parquet  0.1127     0.1914    10000000  yes        yes
numba    1.0e+08    groupby    parquet       -          -           -   no        yes
```
