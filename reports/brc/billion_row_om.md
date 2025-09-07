# Billion Row OM Runner

Generated at: 2025-09-07 07:48:19

```text
backend  rows(sci)  read_s  compute_s  input_rows   ok
-------  ---------  ------  ---------  ----------  ---
pandas   1.0e+03    0.0011     0.0024        1000  yes
pandas   1.0e+04    0.0020     0.0022       10000  yes
pandas   1.0e+05    0.0142     0.0028      100000  yes
pandas   1.0e+06    0.1096     0.0074     1000000  yes
pandas   1.0e+07    0.6841     0.0381    10000000  yes
pandas   1.0e+08    0.6962     0.0399   100000000  yes
dask     1.0e+03    0.0036     0.0075        1000  yes
dask     1.0e+04    0.0039     0.0086       10000  yes
dask     1.0e+05    0.0040     0.0202      100000  yes
dask     1.0e+06    0.0040     0.1249     1000000  yes
dask     1.0e+07    0.0040     0.2721    10000000  yes
dask     1.0e+08    0.0042     0.2765   100000000  yes
pyspark  1.0e+03    4.0216     0.2562        1000  yes
pyspark  1.0e+04    3.9769     0.2644       10000  yes
pyspark  1.0e+05    4.0821     0.4632      100000  yes
pyspark  1.0e+06    4.2009     1.8217     1000000  yes
pyspark  1.0e+07         -          -           -   no
polars   1.0e+03    0.3439     0.0027        1000  yes
polars   1.0e+04    0.3088     0.0030       10000  yes
polars   1.0e+05    0.3269     0.0032      100000  yes
polars   1.0e+06    0.3606     0.0104     1000000  yes
polars   1.0e+07    0.4560     0.0502    10000000  yes
polars   1.0e+08    0.4699     0.0511   100000000  yes
duckdb   1.0e+03    0.2998     0.0025        1000  yes
duckdb   1.0e+04    0.3371     0.0025       10000  yes
duckdb   1.0e+05    0.3456     0.0029      100000  yes
duckdb   1.0e+06    0.4179     0.0080     1000000  yes
duckdb   1.0e+07    0.5808     0.0381    10000000  yes
duckdb   1.0e+08    0.5757     0.0390   100000000  yes
```
