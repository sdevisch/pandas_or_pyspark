# unipandas benchmark

## Run context

- Data file: `/Users/sdevisch/repos/pandas_or_pyspark/data/smoke.csv`
- Ran at: 2025-09-06 16:46:29
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
pandas   2.2.2     0.0008     0.0000     1           1
dask     2024.5.1  0.0031     0.0081     1          11
pyspark  3.5.1     3.8205     0.4447     1          11
polars   1.33.0    0.0024     0.0000     1        None
duckdb   1.3.2     0.0090     0.0000     1        None
```
