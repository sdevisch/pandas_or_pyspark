# Billion Row Challenge (scaffold) - groupby

Generated at: 2025-09-21 14:10:44

- operation: groupby
- materialize: count
- num_chunks: 1
- total_bytes: 106530
- source: parquet
- input_rows: 10000
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11

```text
backend  version   op       read_s  compute_s   rows  used_cores  groups
-------  --------  -------  ------  ---------  -----  ----------  ------
numba    0.60.0    groupby  0.2010     0.0009  10000           1       3
pandas   2.2.2     groupby       -          -      -           -       -
dask     2024.5.1  groupby       -          -      -           -       -
pyspark  3.5.1     groupby       -          -      -           -       -
polars   1.33.0    groupby       -          -      -           -       -
duckdb   1.3.2     groupby       -          -      -           -       -
numpy    1.26.4    groupby       -          -      -           -       -
```

Groupby result preview (by backend):

```text
backend  cat       x                    y
-------  ---  ------  -------------------
numba    x     25733  -11.969814704124328
numba    y    -60499  -11.777341389728097
numba    z     65563     9.92494019138756
backend  cat       x                    y
-------  ---  ------  -------------------
pandas   x     25733  -11.969814704124328
pandas   y    -60499  -11.777341389728097
pandas   z     65563     9.92494019138756
backend  cat       x                    y
-------  ---  ------  -------------------
dask     x     25733  -11.969814704124328
dask     y    -60499  -11.777341389728097
dask     z     65563     9.92494019138756
backend  cat       x                    y
-------  ---  ------  -------------------
pyspark  x     25733  -11.969814704124328
pyspark  y    -60499  -11.777341389728097
pyspark  z     65563     9.92494019138756
backend  cat       x                    y
-------  ---  ------  -------------------
polars   x     25733  -11.969814704124328
polars   y    -60499  -11.777341389728097
polars   z     65563     9.92494019138756
backend  cat       x                    y
-------  ---  ------  -------------------
duckdb   x     25733  -11.969814704124328
duckdb   y    -60499  -11.777341389728097
duckdb   z     65563     9.92494019138756
backend  cat       x                    y
-------  ---  ------  -------------------
numpy    x     25733  -11.969814704124328
numpy    y    -60499  -11.777341389728097
numpy    z     65563     9.92494019138756
```
