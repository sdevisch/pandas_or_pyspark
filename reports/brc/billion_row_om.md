# Billion Row OM Runner

Generated at: 2025-09-07 09:59:57

```text
backend  rows(sci)  operation  source     read_s  compute_s  input_rows   ok  sanity_ok
-------  ---------  ---------  ---------  ------  ---------  ----------  ---  ---------
pandas   1.0e+02    filter     generated  0.0009     0.0026         100  yes        yes
pandas   1.0e+03    filter     parquet    0.0267     0.0030        1000  yes        yes
pandas   1.0e+04    filter     parquet    0.0255     0.0029       10000  yes        yes
pandas   1.0e+05    filter     parquet    0.0262     0.0032      100000  yes        yes
pandas   1.0e+06    filter     parquet    0.0374     0.0083     1000000  yes        yes
pandas   1.0e+07    filter     csv        0.7216     0.0399     6416532  yes        yes
pandas   1.0e+08    filter     csv        0.7339     0.0399     6561303  yes        yes
dask     1.0e+02    filter     parquet    0.0034     0.0064         100  yes        yes
dask     1.0e+03    filter     parquet    0.0034     0.0063        1000  yes        yes
dask     1.0e+04    filter     parquet    0.0033     0.0065       10000  yes        yes
dask     1.0e+05    filter     parquet    0.0033     0.0092      100000  yes        yes
dask     1.0e+06    filter     parquet    0.0032     0.0351     1000000  yes        yes
dask     1.0e+07    filter     csv        0.0040     0.2802     6416532  yes        yes
dask     1.0e+08    filter     csv        0.0041     0.2868     6561303  yes        yes
pyspark  1.0e+02    filter     parquet    3.3921     0.8619         100  yes        yes
pyspark  1.0e+03    filter     parquet    3.1863     0.9177        1000  yes        yes
pyspark  1.0e+04    filter     parquet    3.2228     1.0282       10000  yes        yes
pyspark  1.0e+05    filter     parquet    3.1430     1.0205      100000  yes        yes
pyspark  1.0e+06    filter     parquet    3.2658     2.4057     1000000  yes        yes
pyspark  1.0e+07    filter     csv             -          -           -   no        yes
polars   1.0e+02    filter     parquet    0.3090     0.0024         100  yes        yes
polars   1.0e+03    filter     parquet    0.3417     0.0025        1000  yes        yes
polars   1.0e+04    filter     parquet    0.3074     0.0028       10000  yes        yes
polars   1.0e+05    filter     parquet    0.3559     0.0031      100000  yes        yes
polars   1.0e+06    filter     parquet    0.3573     0.0104     1000000  yes        yes
polars   1.0e+07    filter     csv        0.5565     0.0523     6416532  yes        yes
polars   1.0e+08    filter     csv        0.4652     0.0543     6561303  yes        yes
duckdb   1.0e+02    filter     parquet    0.3277     0.0024         100  yes        yes
duckdb   1.0e+03    filter     parquet    0.3266     0.0025        1000  yes        yes
duckdb   1.0e+04    filter     parquet    0.3473     0.0025       10000  yes        yes
duckdb   1.0e+05    filter     parquet    0.3142     0.0035      100000  yes        yes
duckdb   1.0e+06    filter     parquet    0.3606     0.0087     1000000  yes        yes
duckdb   1.0e+07    filter     csv        0.6245     0.0385     6416532  yes        yes
duckdb   1.0e+08    filter     csv        0.6434     0.0416     6561303  yes        yes
```
