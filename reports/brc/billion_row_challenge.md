# Billion Row Challenge (scaffold) - groupby

Generated at: 2025-09-08 16:31:30

- operation: groupby
- materialize: count
- num_chunks: 1
- total_bytes: 125832454
- source: csv
- input_rows: -
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11

```text
backend  version        op  read_s  compute_s  rows  used_cores  groups
-------  --------  -------  ------  ---------  ----  ----------  ------
pandas   2.2.2     groupby  0.7247     0.1227     -           1       3
dask     2024.5.1  groupby  0.0054     0.4375     -          11       3
pyspark  3.5.1     groupby  5.2875     1.6609     -          11       3
polars   1.33.0    groupby  0.2918     0.1853     -           1       3
duckdb   1.3.2     groupby  0.3385     0.1475     -           1       3
```

Groupby result preview (by backend):

```text
backend  cat        x                    y
-------  ---  -------  -------------------
pandas   x    -252168  0.03277060575968222
pandas   y    -698777   0.5424896951870958
pandas   z     160750  -0.5732209474285132
backend  cat        x                    y
-------  ---  -------  -------------------
dask     x    -252168  0.03277060575968222
dask     y    -698777   0.5424896951870958
dask     z     160750  -0.5732209474285132
backend  cat        x                    y
-------  ---  -------  -------------------
pyspark  x    -252168  0.03277060575968222
pyspark  y    -698777   0.5424896951870958
pyspark  z     160750  -0.5732209474285132
backend  cat        x                    y
-------  ---  -------  -------------------
polars   x    -252168  0.03277060575968222
polars   y    -698777   0.5424896951870958
polars   z     160750  -0.5732209474285132
backend  cat        x                    y
-------  ---  -------  -------------------
duckdb   x    -252168  0.03277060575968222
duckdb   y    -698777   0.5424896951870958
duckdb   z     160750  -0.5732209474285132
```
