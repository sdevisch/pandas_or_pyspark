# Billion Row Challenge (scaffold) - groupby

Generated at: 2025-09-08 09:52:28

- operation: groupby
- materialize: count
- num_chunks: 1
- total_bytes: 7306749
- source: parquet
- input_rows: 1000000
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11

```text
backend  version        op  read_s  compute_s     rows  used_cores  groups
-------  --------  -------  ------  ---------  -------  ----------  ------
pandas   2.2.2     groupby  0.0144     0.0194  1000000           1       3
dask     2024.5.1  groupby  0.0036     0.0427  1000000          11       3
pyspark  3.5.1     groupby  3.0722     1.1503  1000000          11       3
polars   1.33.0    groupby  0.0264     0.0230  1000000           1       3
duckdb   1.3.2     groupby  0.0393     0.0207  1000000           1       3
```

Groupby result preview (by backend):

```text
backend  cat        x                     y
-------  ---  -------  --------------------
pandas   x      96487  -0.16590730040050206
pandas   y    -180846      -0.9841791429969
pandas   z     638169   -1.0642281746150934
backend  cat        x                     y
-------  ---  -------  --------------------
dask     x      96487  -0.16590730040050206
dask     y    -180846      -0.9841791429969
dask     z     638169   -1.0642281746150934
backend  cat        x                     y
-------  ---  -------  --------------------
pyspark  x      96487  -0.16590730040050206
pyspark  y    -180846      -0.9841791429969
pyspark  z     638169   -1.0642281746150934
backend  cat        x                     y
-------  ---  -------  --------------------
polars   x      96487  -0.16590730040050206
polars   y    -180846      -0.9841791429969
polars   z     638169   -1.0642281746150934
backend  cat        x                     y
-------  ---  -------  --------------------
duckdb   x      96487  -0.16590730040050206
duckdb   y    -180846      -0.9841791429969
duckdb   z     638169   -1.0642281746150934
```
