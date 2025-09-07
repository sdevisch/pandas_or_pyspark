# Billion Row OM Runner

Generated at: 2025-09-07 13:09:23

```text
backend  rows(sci)  operation  source   read_s  compute_s  input_rows   ok  sanity_ok
-------  ---------  ---------  -------  ------  ---------  ----------  ---  ---------
pandas   1.0e+02    groupby    parquet  0.3610     0.0007         100  yes        yes
pandas   1.0e+03    groupby    parquet  0.0013     0.0006        1000  yes        yes
pandas   1.0e+04    groupby    parquet  0.0015     0.0008       10000  yes        yes
pandas   1.0e+05    groupby    parquet  0.0028     0.0025      100000  yes        yes
pandas   1.0e+06    groupby    parquet  0.0153     0.0205     1000000  yes        yes
pandas   1.0e+07    groupby    parquet  0.1120     0.2010    10000000  yes        yes
pandas   1.0e+08    groupby    parquet       -          -           -   no        yes
dask     1.0e+02    groupby    parquet  0.2264     0.0179         100  yes        yes
dask     1.0e+03    groupby    parquet  0.0015     0.0162        1000  yes        yes
dask     1.0e+04    groupby    parquet  0.0012     0.0165       10000  yes        yes
dask     1.0e+05    groupby    parquet  0.0014     0.0199      100000  yes        yes
dask     1.0e+06    groupby    parquet  0.0016     0.0469     1000000  yes        yes
dask     1.0e+07    groupby    parquet  0.0018     0.3022    10000000  yes        yes
dask     1.0e+08    groupby    parquet  0.0191     1.3575   190000000  yes        yes
dask     1.0e+09    groupby    parquet       -          -           -   no        yes
pyspark  1.0e+02    groupby    parquet  3.9974     1.2215         100  yes        yes
pyspark  1.0e+03    groupby    parquet  3.2029     1.1814        1000  yes        yes
pyspark  1.0e+04    groupby    parquet  3.4407     1.1752       10000  yes        yes
pyspark  1.0e+05    groupby    parquet  3.6575     1.2463      100000  yes        yes
pyspark  1.0e+06    groupby    parquet  3.2624     1.2493     1000000  yes        yes
pyspark  1.0e+07    groupby    parquet  3.1157     1.3423    10000000  yes        yes
pyspark  1.0e+08    groupby    parquet       -          -           -   no        yes
polars   1.0e+02    groupby    parquet  0.1443     0.0010         100  yes        yes
polars   1.0e+03    groupby    parquet  0.0036     0.0005        1000  yes        yes
polars   1.0e+04    groupby    parquet  0.0023     0.0008       10000  yes        yes
polars   1.0e+05    groupby    parquet  0.0039     0.0029      100000  yes        yes
polars   1.0e+06    groupby    parquet  0.0255     0.0225     1000000  yes        yes
polars   1.0e+07    groupby    parquet  0.1637     0.2158    10000000  yes        yes
polars   1.0e+08    groupby    parquet       -          -           -   no        yes
duckdb   1.0e+02    groupby    parquet  0.0414     0.0009         100  yes        yes
duckdb   1.0e+03    groupby    parquet  0.0066     0.0007        1000  yes        yes
duckdb   1.0e+04    groupby    parquet  0.0082     0.0010       10000  yes        yes
duckdb   1.0e+05    groupby    parquet  0.0109     0.0029      100000  yes        yes
duckdb   1.0e+06    groupby    parquet  0.0397     0.0224     1000000  yes        yes
duckdb   1.0e+07    groupby    parquet  0.2096     0.2044    10000000  yes        yes
duckdb   1.0e+08    groupby    parquet       -          -           -   no        yes
```
