# Billion Row Challenge (scaffold) - groupby

Generated at: 2025-09-09 08:17:12

- operation: groupby
- materialize: count
- num_chunks: 20
- total_bytes: 1982831425
- source: csv
- input_rows: -
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11

```text
backend  version        op   read_s  compute_s  rows  used_cores  groups
-------  --------  -------  -------  ---------  ----  ----------  ------
pandas   2.2.2     groupby  12.2807     1.9636     -           1       3
dask     2024.5.1  groupby   0.0666     5.0154     -          11       3
pyspark  3.5.1     groupby  16.3454    10.9215     -          11       3
polars   1.33.0    groupby   3.6863     2.1047     -           1       3
duckdb   1.3.2     groupby   5.0220     2.2552     -           1       3
```

Groupby result preview (by backend):

```text
backend  cat         x                      y
-------  ---  --------  ---------------------
pandas   x     2784549   0.031530096249204706
pandas   y    -5889758  -0.045692206867267514
pandas   z    -2438550   -0.13857623393181231
backend  cat         x                      y
-------  ---  --------  ---------------------
dask     x     2784549   0.031530096249204706
dask     y    -5889758  -0.045692206867267514
dask     z    -2438550   -0.13857623393181231
backend  cat         x                      y
-------  ---  --------  ---------------------
pyspark  x     2784549   0.031530096249204706
pyspark  y    -5889758  -0.045692206867267514
pyspark  z    -2438550   -0.13857623393181231
backend  cat         x                      y
-------  ---  --------  ---------------------
polars   x     2784549   0.031530096249204706
polars   y    -5889758  -0.045692206867267514
polars   z    -2438550   -0.13857623393181231
backend  cat         x                      y
-------  ---  --------  ---------------------
duckdb   x     2784549   0.031530096249204706
duckdb   y    -5889758  -0.045692206867267514
duckdb   z    -2438550   -0.13857623393181231
```
