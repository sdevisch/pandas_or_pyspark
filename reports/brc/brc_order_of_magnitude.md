# Billion Row OM Runner

Generated at: 2025-09-21 14:10:03

```text
backend  rows(sci)  operation  source   read_s  compute_s  input_rows   ok  sanity_ok
-------  ---------  ---------  -------  ------  ---------  ----------  ---  ---------
pandas   1.0e+02    groupby    parquet  0.3314     0.0007         100  yes        yes
pandas   1.0e+03    groupby    parquet  0.0015     0.0006        1000  yes        yes
pandas   1.0e+04    groupby    parquet  0.0013     0.0006       10000  yes        yes
pandas   1.0e+05    groupby    parquet  0.0028     0.0026      100000  yes        yes
pandas   1.0e+06    groupby    parquet  0.0154     0.0432     1000000  yes        yes
pandas   1.0e+07    groupby    parquet  0.1146     0.2023    10000000  yes        yes
pandas   1.0e+08    groupby    parquet       -          -           -   no        yes
dask     1.0e+02    groupby    parquet  0.2073     0.0161         100  yes        yes
dask     1.0e+03    groupby    parquet  0.0016     0.0155        1000  yes        yes
dask     1.0e+04    groupby    parquet  0.0013     0.0154       10000  yes        yes
dask     1.0e+05    groupby    parquet  0.0015     0.0183      100000  yes        yes
dask     1.0e+06    groupby    parquet  0.0014     0.0440     1000000  yes        yes
dask     1.0e+07    groupby    parquet  0.0017     0.2890    10000000  yes        yes
dask     1.0e+08    groupby    parquet  0.0178     1.5298   190000000  yes        yes
dask     1.0e+09    groupby    parquet       -          -           -   no        yes
pyspark  1.0e+02    groupby    parquet       -          -           -   no        yes
polars   1.0e+02    groupby    parquet  0.1424     0.0012         100  yes        yes
polars   1.0e+03    groupby    parquet  0.0036     0.0005        1000  yes        yes
polars   1.0e+04    groupby    parquet  0.0033     0.0007       10000  yes        yes
polars   1.0e+05    groupby    parquet  0.0061     0.0033      100000  yes        yes
polars   1.0e+06    groupby    parquet  0.0335     0.0211     1000000  yes        yes
polars   1.0e+07    groupby    parquet  0.1518     0.1994    10000000  yes        yes
polars   1.0e+08    groupby    parquet       -          -           -   no        yes
duckdb   1.0e+02    groupby    parquet  0.0846     0.0007         100  yes        yes
duckdb   1.0e+03    groupby    parquet  0.0059     0.0006        1000  yes        yes
duckdb   1.0e+04    groupby    parquet  0.0063     0.0014       10000  yes        yes
duckdb   1.0e+05    groupby    parquet  0.0086     0.0024      100000  yes        yes
duckdb   1.0e+06    groupby    parquet  0.0341     0.0200     1000000  yes        yes
duckdb   1.0e+07    groupby    parquet  0.1796     0.1909    10000000  yes        yes
duckdb   1.0e+08    groupby    parquet       -          -           -   no        yes
numpy    1.0e+02    groupby    parquet  0.0131     0.0006         100  yes        yes
numpy    1.0e+03    groupby    parquet  0.0019     0.0005        1000  yes        yes
numpy    1.0e+04    groupby    parquet  0.0029     0.0008       10000  yes        yes
numpy    1.0e+05    groupby    parquet  0.0053     0.0026      100000  yes        yes
numpy    1.0e+06    groupby    parquet  0.0391     0.0201     1000000  yes        yes
numpy    1.0e+07    groupby    parquet  0.1778     0.1917    10000000  yes        yes
numpy    1.0e+08    groupby    parquet       -          -           -   no        yes
numba    1.0e+02    groupby    parquet  0.0033     0.0007         100  yes        yes
numba    1.0e+03    groupby    parquet  0.0011     0.0005        1000  yes        yes
numba    1.0e+04    groupby    parquet  0.0022     0.0007       10000  yes        yes
numba    1.0e+05    groupby    parquet  0.0023     0.0024      100000  yes        yes
numba    1.0e+06    groupby    parquet  0.0145     0.0199     1000000  yes        yes
numba    1.0e+07    groupby    parquet  0.1095     0.1890    10000000  yes        yes
numba    1.0e+08    groupby    parquet       -          -           -   no        yes
```
