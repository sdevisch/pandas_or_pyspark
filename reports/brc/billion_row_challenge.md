# Billion Row Challenge (scaffold)

Generated at: 2025-09-07 17:03:01

- operation: groupby
- materialize: count
- num_chunks: 100
- total_bytes: 7305361961
- source: parquet
- input_rows: 1000000000

```text
backend  version        op  read_s  compute_s  rows  used_cores
-------  --------  -------  ------  ---------  ----  ----------
pyspark  3.5.1     groupby  9.3817    21.3128     3          11
pandas   2.2.2     groupby  0.0000     0.0000  None        None
dask     2024.5.1  groupby  0.0000     0.0000  None        None
polars   1.33.0    groupby  0.0000     0.0000  None        None
duckdb   1.3.2     groupby  0.0000     0.0000  None        None
```

Groupby result preview:

```text
cat  x                            y
---  --------  --------------------
x    12679730  -0.04973974533733489
z    -5784388  0.015786534905467282
y    9464519       0.05088346148381
```
