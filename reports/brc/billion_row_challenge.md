# Billion Row Challenge (scaffold) - filter

Generated at: 2025-09-07 17:35:48

- operation: filter
- materialize: count
- num_chunks: 100
- total_bytes: 7305361961
- source: parquet
- input_rows: 1000000000
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11

```text
backend  version       op    read_s  compute_s       rows  used_cores
-------  --------  ------  --------  ---------  ---------  ----------
pandas   2.2.2     filter  123.4037   175.7183  249756815           1
dask     2024.5.1  filter    0.3192    27.5590  249756815          11
pyspark  3.5.1     filter   11.0622    12.1276  249756815          11
polars   1.33.0    filter  135.4495   167.6631  249756815           1
duckdb   1.3.2     filter  129.7218   174.4886  249756815           1
```


# Billion Row Challenge (scaffold) - groupby

Generated at: 2025-09-07 17:50:16

- operation: groupby
- materialize: count
- num_chunks: 100
- total_bytes: 7305361961
- source: parquet
- input_rows: 1000000000
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11

```text
backend  version        op    read_s  compute_s  rows  used_cores
-------  --------  -------  --------  ---------  ----  ----------
pandas   2.2.2     groupby  108.9645   144.2124     3           1
dask     2024.5.1  groupby    0.4879    17.7692     3          11
pyspark  3.5.1     groupby    6.8663    21.8989     3          11
polars   1.33.0    groupby  137.7771   137.4502     3           1
duckdb   1.3.2     groupby  129.2748   132.0518     3           1
```

Groupby result preview:

```text
cat  x                            y
---  --------  --------------------
x    12679730  -0.04973974533733489
y    9464519       0.05088346148381
z    -5784388  0.015786534905467282
```
