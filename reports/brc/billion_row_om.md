# Billion Row OM Runner

Generated at: 2025-09-07 09:33:19

```text
backend  rows(sci)  read_s  compute_s  input_rows   ok
-------  ---------  ------  ---------  ----------  ---
pandas   1.0e+03    0.0012     0.0026        1000  yes
pandas   1.0e+04    0.0025     0.0031       10000  yes
pandas   1.0e+05    0.0136     0.0031      100000  yes
pandas   1.0e+06    0.1179     0.0090     1000000  yes
pandas   1.0e+07    0.7335     0.0394    10000000  yes
pandas   1.0e+08    0.7491     0.0416   100000000  yes
dask     1.0e+03    0.0041     0.0078        1000  yes
dask     1.0e+04    0.0043     0.0095       10000  yes
dask     1.0e+05    0.0048     0.0227      100000  yes
dask     1.0e+06    0.0045     0.1352     1000000  yes
dask     1.0e+07    0.0045     0.2825    10000000  yes
dask     1.0e+08    0.0044     0.2910   100000000  yes
pyspark  1.0e+03    4.2189     0.2657        1000  yes
pyspark  1.0e+04    4.1128     0.2972       10000  yes
pyspark  1.0e+05    4.5170     0.4601      100000  yes
pyspark  1.0e+06    4.3745     1.8553     1000000  yes
pyspark  1.0e+07         -          -           -   no
polars   1.0e+03    0.3343     0.0027        1000  yes
polars   1.0e+04    0.3376     0.0027       10000  yes
polars   1.0e+05    0.3312     0.0038      100000  yes
polars   1.0e+06    0.3506     0.0101     1000000  yes
polars   1.0e+07    0.4773     0.0523    10000000  yes
polars   1.0e+08    0.4753     0.0533   100000000  yes
duckdb   1.0e+03    0.3460     0.0024        1000  yes
duckdb   1.0e+04    0.3421     0.0026       10000  yes
duckdb   1.0e+05    0.3846     0.0029      100000  yes
duckdb   1.0e+06    0.4305     0.0086     1000000  yes
duckdb   1.0e+07    0.6135     0.0408    10000000  yes
duckdb   1.0e+08    0.6055     0.0414   100000000  yes
```
