# unipandas benchmark

## Run context

- Data file: `data/example.csv`
- Ran at: 2025-09-06 12:04:04
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- System available cores: 11
- Args: assign=True, query='a > 0', groupby='cat'

## Backend availability

| backend | version | status |
|---|---|---|
| pandas | 2.2.2 | available |
| dask | 2024.5.1 | available |
| pyspark | 3.5.1 | available |
| polars | 1.33.0 | available |
| duckdb | 1.3.2 | available |

## Results (seconds)

```text
backend  version   load_s  compute_s  rows  used_cores
-------  --------  ------  ---------  ----  ----------
pandas   2.2.2     0.0218     0.0000     3           1
dask     2024.5.1  0.0050     0.0355     3          11
pyspark  3.5.1     4.0448     0.7087     3          11
polars   1.33.0    0.0070     0.0000     3        None
duckdb   1.3.2     0.0485     0.0000     3        None
```
