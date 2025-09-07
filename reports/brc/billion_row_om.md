# Billion Row OM Runner

Generated at: 2025-09-07 10:06:18

```text
backend  rows(sci)  operation  source   read_s  compute_s  input_rows   ok  sanity_ok
-------  ---------  ---------  -------  ------  ---------  ----------  ---  ---------
pandas   1.0e+02    filter     parquet  0.0521     0.0029         100  yes        yes
pandas   1.0e+03    filter     parquet  0.0283     0.0028        1000  yes        yes
pandas   1.0e+04    filter     parquet  0.0262     0.0026       10000  yes        yes
pandas   1.0e+05    filter     parquet  0.0283     0.0035      100000  yes        yes
pandas   1.0e+06    filter     parquet  0.0401     0.0087     1000000  yes        yes
pandas   1.0e+07    filter     parquet  0.1368     0.0644    10000000  yes        yes
pandas   1.0e+08    filter     parquet       -          -           -   no        yes
dask     1.0e+02    filter     parquet  0.0082     0.0072         100  yes        yes
dask     1.0e+03    filter     parquet  0.0038     0.0065        1000  yes        yes
dask     1.0e+04    filter     parquet  0.0040     0.0071       10000  yes        yes
dask     1.0e+05    filter     parquet  0.0040     0.0099      100000  yes        yes
dask     1.0e+06    filter     parquet  0.0043     0.0348     1000000  yes        yes
dask     1.0e+07    filter     parquet  0.0034     0.2858    10000000  yes        yes
dask     1.0e+08    filter     parquet  0.0202     1.8669   190000000  yes        yes
pyspark  1.0e+02    filter     parquet  4.0291     0.8967         100  yes        yes
pyspark  1.0e+03    filter     parquet  3.1508     0.8730        1000  yes        yes
pyspark  1.0e+04    filter     parquet  3.1644     0.9087       10000  yes        yes
pyspark  1.0e+05    filter     parquet  3.1491     1.0099      100000  yes        yes
pyspark  1.0e+06    filter     parquet  3.1819     3.0228     1000000  yes        yes
pyspark  1.0e+07    filter     parquet       -          -           -   no        yes
polars   1.0e+02    filter     parquet  0.3410     0.0023         100  yes        yes
polars   1.0e+03    filter     parquet  0.2997     0.0024        1000  yes        yes
polars   1.0e+04    filter     parquet  0.3278     0.0024       10000  yes        yes
polars   1.0e+05    filter     parquet  0.3350     0.0031      100000  yes        yes
polars   1.0e+06    filter     parquet  0.3276     0.0079     1000000  yes        yes
polars   1.0e+07    filter     parquet  0.4349     0.0585    10000000  yes        yes
polars   1.0e+08    filter     parquet  5.2443     2.5383   190000000  yes        yes
duckdb   1.0e+02    filter     parquet  0.3620     0.0024         100  yes        yes
duckdb   1.0e+03    filter     parquet  0.3189     0.0024        1000  yes        yes
duckdb   1.0e+04    filter     parquet  0.3399     0.0025       10000  yes        yes
duckdb   1.0e+05    filter     parquet  0.3374     0.0030      100000  yes        yes
duckdb   1.0e+06    filter     parquet  0.3683     0.0078     1000000  yes        yes
duckdb   1.0e+07    filter     parquet  0.4829     0.0554    10000000  yes        yes
duckdb   1.0e+08    filter     parquet  6.3528     2.1895   190000000  yes        yes
```
