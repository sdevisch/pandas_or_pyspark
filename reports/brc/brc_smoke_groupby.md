# Billion Row Challenge (scaffold) - groupby

Generated at: 2025-09-20 13:14:23

- operation: groupby
- materialize: count
- num_chunks: 1
- total_bytes: 73052334
- source: parquet
- input_rows: 10000000
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11

```text
backend  version   op       read_s  compute_s      rows  used_cores  groups
-------  --------  -------  ------  ---------  --------  ----------  ------
pandas   2.2.2     groupby  0.1205     0.1878  10000000           1       3
dask     2024.5.1  groupby       -          -         -           -       -
pyspark  3.5.1     groupby       -          -         -           -       -
polars   1.33.0    groupby       -          -         -           -       -
duckdb   1.3.2     groupby       -          -         -           -       -
numpy    1.26.4    groupby       -          -         -           -       -
numba    0.60.0    groupby       -          -         -           -       -
```

Groupby result preview (by backend):

```text
backend  cat         x                     y
-------  ---  --------  --------------------
pandas   x    -1731397    -0.450981404090764
pandas   y      337765  -0.19284947854156623
pandas   z     -726934   0.19171086572356014
backend  cat         x                     y
-------  ---  --------  --------------------
dask     x    -1731397    -0.450981404090764
dask     y      337765  -0.19284947854156623
dask     z     -726934   0.19171086572356014
backend  cat         x                     y
-------  ---  --------  --------------------
pyspark  x    -1731397    -0.450981404090764
pyspark  y      337765  -0.19284947854156623
pyspark  z     -726934   0.19171086572356014
backend  cat         x                     y
-------  ---  --------  --------------------
polars   x    -1731397    -0.450981404090764
polars   y      337765  -0.19284947854156623
polars   z     -726934   0.19171086572356014
backend  cat         x                     y
-------  ---  --------  --------------------
duckdb   x    -1731397    -0.450981404090764
duckdb   y      337765  -0.19284947854156623
duckdb   z     -726934   0.19171086572356014
backend  cat         x                     y
-------  ---  --------  --------------------
numpy    x    -1731397    -0.450981404090764
numpy    y      337765  -0.19284947854156623
numpy    z     -726934   0.19171086572356014
backend  cat         x                     y
-------  ---  --------  --------------------
numba    x    -1731397    -0.450981404090764
numba    y      337765  -0.19284947854156623
numba    z     -726934   0.19171086572356014
```
