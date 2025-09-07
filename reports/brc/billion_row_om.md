# Billion Row OM Runner

Generated at: 2025-09-07 09:52:18

```text
backend  rows(sci)  operation  source  read_s  compute_s  input_rows   ok  sanity_ok
-------  ---------  ---------  ------  ------  ---------  ----------  ---  ---------
pandas   1.0e+03    filter     csv     0.0011     0.0025        1000  yes        yes
pandas   1.0e+04    filter     csv     0.0020     0.0026       10000  yes        yes
pandas   1.0e+05    filter     csv     0.0139     0.0030      100000  yes        yes
pandas   1.0e+06    filter     csv     0.1125     0.0077     1000000  yes        yes
pandas   1.0e+07    filter     csv     0.7223     0.0425     6416532  yes        yes
pandas   1.0e+08    filter     csv     0.7428     0.0393     6561303  yes        yes
dask     1.0e+03    filter     csv     0.0039     0.0081        1000  yes        yes
dask     1.0e+04    filter     csv     0.0044     0.0091       10000  yes        yes
dask     1.0e+05    filter     csv     0.0042     0.0216      100000  yes        yes
dask     1.0e+06    filter     csv     0.0044     0.1293     1000000  yes        yes
dask     1.0e+07    filter     csv     0.0042     0.3334     6416532  yes        yes
dask     1.0e+08    filter     csv     0.0044     0.2843     6561303  yes        yes
pyspark  1.0e+03    filter     csv     3.9374     0.2576        1000  yes        yes
pyspark  1.0e+04    filter     csv     4.0030     0.2779       10000  yes        yes
pyspark  1.0e+05    filter     csv     4.0783     0.4507      100000  yes        yes
pyspark  1.0e+06    filter     csv     4.2380     1.7736     1000000  yes        yes
pyspark  1.0e+07    filter     csv          -          -           -   no        yes
polars   1.0e+03    filter     csv     0.3294     0.0027        1000  yes        yes
polars   1.0e+04    filter     csv     0.3121     0.0027       10000  yes        yes
polars   1.0e+05    filter     csv     0.3310     0.0037      100000  yes        yes
polars   1.0e+06    filter     csv     0.3443     0.0088     1000000  yes        yes
polars   1.0e+07    filter     csv     0.4602     0.0505     6416532  yes        yes
polars   1.0e+08    filter     csv     0.4506     0.0518     6561303  yes        yes
duckdb   1.0e+03    filter     csv     0.3226     0.0025        1000  yes        yes
duckdb   1.0e+04    filter     csv     0.3384     0.0025       10000  yes        yes
duckdb   1.0e+05    filter     csv     0.3782     0.0029      100000  yes        yes
duckdb   1.0e+06    filter     csv     0.4192     0.0081     1000000  yes        yes
duckdb   1.0e+07    filter     csv     0.5786     0.0385     6416532  yes        yes
duckdb   1.0e+08    filter     csv     0.5889     0.0398     6561303  yes        yes
```
