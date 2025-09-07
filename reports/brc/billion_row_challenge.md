# Billion Row Challenge (scaffold)

Generated at: 2025-09-07 16:48:41

- operation: groupby
- materialize: count
- num_chunks: 1
- total_bytes: 7306749
- source: parquet
- input_rows: 1000000

```text
backend  version        op  read_s  compute_s  rows  used_cores
-------  --------  -------  ------  ---------  ----  ----------
pandas   2.2.2     groupby  0.0244     0.0204     3           1
dask     2024.5.1  groupby  0.0062     0.0464     3          11
pyspark  3.5.1     groupby  3.5933     1.1172     3          11
polars   1.33.0    groupby  0.0268     0.0229     3           1
duckdb   1.3.2     groupby  0.0403     0.0193     3           1
```

Groupby result preview:

```text
cat  x                           y
---  -------  --------------------
x    96487    -0.16590730040050206
y    -180846      -0.9841791429969
z    638169    -1.0642281746150934
```
