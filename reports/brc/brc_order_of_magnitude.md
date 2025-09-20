# Billion Row OM Runner

Generated at: 2025-09-20 09:51:02

```text
backend  rows(sci)  operation  source   read_s  compute_s  input_rows   ok  sanity_ok
-------  ---------  ---------  -------  ------  ---------  ----------  ---  ---------
pandas   1.0e+02    groupby    parquet  0.3234     0.0007         100  yes        yes
pandas   1.0e+03    groupby    parquet  0.0009     0.0005        1000  yes        yes
pandas   1.0e+04    groupby    parquet  0.0012     0.0007       10000  yes        yes
pandas   1.0e+05    groupby    parquet  0.0026     0.0023      100000  yes        yes
pandas   1.0e+06    groupby    parquet  0.0139     0.0203     1000000  yes        yes
pandas   1.0e+07    groupby    parquet  0.1016     0.1973    10000000  yes        yes
pandas   1.0e+08    groupby    parquet       -          -           -   no        yes
dask     1.0e+02    groupby    parquet  0.1687     0.0155         100  yes        yes
dask     1.0e+03    groupby    parquet  0.0013     0.0155        1000  yes        yes
dask     1.0e+04    groupby    parquet  0.0013     0.0155       10000  yes        yes
dask     1.0e+05    groupby    parquet  0.0011     0.0181      100000  yes        yes
dask     1.0e+06    groupby    parquet  0.0012     0.0444     1000000  yes        yes
dask     1.0e+07    groupby    parquet  0.0014     0.2847    10000000  yes        yes
dask     1.0e+08    groupby    parquet  0.0154     1.3203   190000000  yes        yes
dask     1.0e+09    groupby    parquet       -          -           -   no        yes
pyspark  1.0e+02    groupby    parquet       -          -           -   no        yes
polars   1.0e+02    groupby    parquet  0.1230     0.0009         100  yes        yes
polars   1.0e+03    groupby    parquet  0.0034     0.0006        1000  yes        yes
polars   1.0e+04    groupby    parquet  0.0052     0.0007       10000  yes        yes
polars   1.0e+05    groupby    parquet  0.0044     0.0026      100000  yes        yes
polars   1.0e+06    groupby    parquet  0.0268     0.0205     1000000  yes        yes
polars   1.0e+07    groupby    parquet  0.1500     0.2028    10000000  yes        yes
polars   1.0e+08    groupby    parquet       -          -           -   no        yes
duckdb   1.0e+02    groupby    parquet  0.0742     0.0006         100  yes        yes
duckdb   1.0e+03    groupby    parquet  0.0062     0.0006        1000  yes        yes
duckdb   1.0e+04    groupby    parquet  0.0062     0.0007       10000  yes        yes
duckdb   1.0e+05    groupby    parquet  0.0091     0.0028      100000  yes        yes
duckdb   1.0e+06    groupby    parquet  0.0337     0.0204     1000000  yes        yes
duckdb   1.0e+07    groupby    parquet  0.1838     0.1914    10000000  yes        yes
duckdb   1.0e+08    groupby    parquet       -          -           -   no        yes
numpy    1.0e+02    groupby    parquet  0.0111     0.0006         100  yes        yes
numpy    1.0e+03    groupby    parquet  0.0026     0.0005        1000  yes        yes
numpy    1.0e+04    groupby    parquet  0.0018     0.0010       10000  yes        yes
numpy    1.0e+05    groupby    parquet  0.0064     0.0030      100000  yes        yes
numpy    1.0e+06    groupby    parquet  0.0449     0.0201     1000000  yes        yes
numpy    1.0e+07    groupby    parquet  0.1924     0.1915    10000000  yes        yes
numpy    1.0e+08    groupby    parquet       -          -           -   no        yes
numba    1.0e+02    groupby    parquet  0.0030     0.0007         100  yes        yes
numba    1.0e+03    groupby    parquet  0.0009     0.0005        1000  yes        yes
numba    1.0e+04    groupby    parquet  0.0011     0.0008       10000  yes        yes
numba    1.0e+05    groupby    parquet  0.0034     0.0026      100000  yes        yes
numba    1.0e+06    groupby    parquet  0.0147     0.0199     1000000  yes        yes
numba    1.0e+07    groupby    parquet  0.1101     0.1912    10000000  yes        yes
numba    1.0e+08    groupby    parquet       -          -           -   no        yes
```
