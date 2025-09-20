# Billion Row Challenge (scaffold) - groupby

Generated at: 2025-09-19 11:08:44

- operation: groupby
- materialize: count
- num_chunks: 100
- total_bytes: 7305361961
- source: parquet
- input_rows: 1000000000
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11

```text
backend  version   op         read_s  compute_s        rows  used_cores  groups
-------  --------  -------  --------  ---------  ----------  ----------  ------
pandas   2.2.2     groupby  126.5253   134.9241  1000000000           1       3
dask     2024.5.1  groupby    0.3680    20.8874  1000000000          11       3
pyspark  3.5.1     groupby    8.6419    23.2958  1000000000          11       3
polars   1.33.0    groupby  137.2226   164.4509  1000000000           1       3
duckdb   1.3.2     groupby  138.7267   153.9890  1000000000           1       3
numpy    1.26.4    groupby  110.9357   131.0384  1000000000           1       3
numba    0.60.0    groupby  125.3475   139.3019  1000000000           1       3
```

Groupby result preview (by backend):

```text
backend  cat         x                     y
-------  ---  --------  --------------------
pandas   x    12679730  -0.04973974533733489
pandas   y     9464519      0.05088346148381
pandas   z    -5784388  0.015786534905467282
backend  cat         x                     y
-------  ---  --------  --------------------
dask     x    12679730  -0.04973974533733489
dask     y     9464519      0.05088346148381
dask     z    -5784388  0.015786534905467282
(preview unavailable)
backend  cat         x                     y
-------  ---  --------  --------------------
polars   x    12679730  -0.04973974533733489
polars   y     9464519      0.05088346148381
polars   z    -5784388  0.015786534905467282
backend  cat         x                     y
-------  ---  --------  --------------------
duckdb   x    12679730  -0.04973974533733489
duckdb   y     9464519      0.05088346148381
duckdb   z    -5784388  0.015786534905467282
backend  cat         x                     y
-------  ---  --------  --------------------
numpy    x    12679730  -0.04973974533733489
numpy    y     9464519      0.05088346148381
numpy    z    -5784388  0.015786534905467282
backend  cat         x                     y
-------  ---  --------  --------------------
numba    x    12679730  -0.04973974533733489
numba    y     9464519      0.05088346148381
numba    z    -5784388  0.015786534905467282
```
