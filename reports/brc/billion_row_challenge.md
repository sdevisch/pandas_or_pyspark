# Billion Row Challenge (scaffold) - groupby

Generated at: 2025-09-08 16:46:47

- operation: groupby
- materialize: count
- num_chunks: 1
- total_bytes: 196728775
- source: csv
- input_rows: -
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11

```text
backend  version        op  read_s  compute_s  rows  used_cores  groups
-------  --------  -------  ------  ---------  ----  ----------  ------
pandas   2.2.2     groupby  1.1444     0.1971     -           1       3
dask     2024.5.1  groupby  0.0059     0.5934     -          11       3
pyspark  3.5.1     groupby  5.2833     1.7535     -          11       3
polars   1.33.0    groupby  0.2268     0.2168     -           1       3
duckdb   1.3.2     groupby  0.4031     0.2009     -           1       3
```

Groupby result preview (by backend):

```text
backend  cat        x                      y
-------  ---  -------  ---------------------
pandas   x    -674346  -0.020533027211690376
pandas   y    -485665     0.2198109565297213
pandas   z     229080    -0.6415656233778314
backend  cat        x                      y
-------  ---  -------  ---------------------
dask     x    -674346  -0.020533027211690376
dask     y    -485665     0.2198109565297213
dask     z     229080    -0.6415656233778314
backend  cat        x                      y
-------  ---  -------  ---------------------
pyspark  x    -674346  -0.020533027211690376
pyspark  y    -485665     0.2198109565297213
pyspark  z     229080    -0.6415656233778314
backend  cat        x                      y
-------  ---  -------  ---------------------
polars   x    -674346  -0.020533027211690376
polars   y    -485665     0.2198109565297213
polars   z     229080    -0.6415656233778314
backend  cat        x                      y
-------  ---  -------  ---------------------
duckdb   x    -674346  -0.020533027211690376
duckdb   y    -485665     0.2198109565297213
duckdb   z     229080    -0.6415656233778314
```
