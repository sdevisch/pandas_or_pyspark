# Billion Row Challenge (scaffold)

Generated at: 2025-09-07 12:42:05

- operation: groupby
- materialize: count
- num_chunks: 100
- total_bytes: 7305361961
- source: parquet
- input_rows: 1000000000

```text
backend  version        op    read_s  compute_s  rows  used_cores
-------  --------  -------  --------  ---------  ----  ----------
pandas   2.2.2     groupby  130.5262   141.4384     3           1
dask     2024.5.1  groupby    0.2365    16.8026     3          11
pyspark  3.5.1     groupby   12.3212    20.6726     3          11
polars   1.33.0    groupby  124.6423   140.2506     3           1
duckdb   1.3.2     groupby  133.2949   130.7061     3           1
```

Groupby result preview:

```text
cat  x                            y
---  --------  --------------------
x    12679730  -0.04973974533733489
y    9464519       0.05088346148381
z    -5784388  0.015786534905467282
```
