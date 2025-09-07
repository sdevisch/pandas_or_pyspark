# Billion Row OM Runner

Generated at: 2025-09-07 08:35:20

```text
backend  rows(sci)  read_s  compute_s  input_rows   ok
-------  ---------  ------  ---------  ----------  ---
pandas   1.0e+03    0.0010     0.0025        1000  yes
pandas   1.0e+04    0.0021     0.0026       10000  yes
pandas   1.0e+05    0.0133     0.0029      100000  yes
pandas   1.0e+06    0.1115     0.0076     1000000  yes
pandas   1.0e+07    0.7094     0.0400    10000000  yes
pandas   1.0e+08    0.7125     0.0382   100000000  yes
dask     1.0e+03    0.0039     0.0075        1000  yes
dask     1.0e+04    0.0041     0.0095       10000  yes
dask     1.0e+05    0.0040     0.0211      100000  yes
dask     1.0e+06    0.0041     0.1288     1000000  yes
dask     1.0e+07    0.0041     0.2795    10000000  yes
dask     1.0e+08    0.0041     0.2729   100000000  yes
pyspark  1.0e+03    3.9061     0.2499        1000  yes
pyspark  1.0e+04    3.9553     0.2849       10000  yes
pyspark  1.0e+05    4.0679     0.5087      100000  yes
pyspark  1.0e+06    4.3849     1.8935     1000000  yes
pyspark  1.0e+07         -          -           -   no
polars   1.0e+03    0.3297     0.0028        1000  yes
polars   1.0e+04    0.3124     0.0028       10000  yes
polars   1.0e+05    0.3163     0.0034      100000  yes
polars   1.0e+06    0.3344     0.0099     1000000  yes
polars   1.0e+07    0.4685     0.0496    10000000  yes
polars   1.0e+08    0.4658     0.0517   100000000  yes
duckdb   1.0e+03    0.3309     0.0025        1000  yes
duckdb   1.0e+04    0.3273     0.0027       10000  yes
duckdb   1.0e+05    0.3531     0.0029      100000  yes
duckdb   1.0e+06    0.4141     0.0083     1000000  yes
duckdb   1.0e+07    0.5697     0.0381    10000000  yes
duckdb   1.0e+08    0.6694     0.0381   100000000  yes
```
