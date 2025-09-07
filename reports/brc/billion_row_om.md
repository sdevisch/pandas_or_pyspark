# Billion Row OM Runner

Generated at: 2025-09-07 07:31:04

```text
backend  rows(sci)  read_s  compute_s  input_rows   ok
-------  ---------  ------  ---------  ----------  ---
pandas   1.0e+03    0.0010     0.0025        1000  yes
pandas   1.0e+04    0.0020     0.0025       10000  yes
pandas   1.0e+05    0.0151     0.0028      100000  yes
pandas   1.0e+06    0.1148     0.0077     1000000  yes
pandas   1.0e+07    0.7047     0.0455    10000000  yes
pandas   1.0e+08    0.7115     0.0422   100000000  yes
dask     1.0e+03    0.0065     0.0080        1000  yes
dask     1.0e+04    0.0040     0.0088       10000  yes
dask     1.0e+05    0.0041     0.0198      100000  yes
dask     1.0e+06    0.0040     0.1242     1000000  yes
dask     1.0e+07    0.0040     0.2717    10000000  yes
dask     1.0e+08    0.0040     0.2729   100000000  yes
pyspark  1.0e+03    4.1691     0.2675        1000  yes
pyspark  1.0e+04    3.8311     0.2843       10000  yes
pyspark  1.0e+05    4.1314     0.4500      100000  yes
pyspark  1.0e+06    4.2417     1.6973     1000000  yes
pyspark  1.0e+07         -          -           -   no
polars   1.0e+03    0.3229     0.0027        1000  yes
polars   1.0e+04    0.3240     0.0031       10000  yes
polars   1.0e+05    0.3174     0.0034      100000  yes
polars   1.0e+06    0.3456     0.0092     1000000  yes
polars   1.0e+07    0.4624     0.0487    10000000  yes
polars   1.0e+08    0.4470     0.0508   100000000  yes
duckdb   1.0e+03    0.3448     0.0025        1000  yes
duckdb   1.0e+04    0.3323     0.0028       10000  yes
duckdb   1.0e+05    0.3657     0.0030      100000  yes
duckdb   1.0e+06    0.4206     0.0081     1000000  yes
duckdb   1.0e+07    0.5791     0.0387    10000000  yes
duckdb   1.0e+08    0.5935     0.0388   100000000  yes
```
