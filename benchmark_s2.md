# unipandas benchmark

## Run context

- Data file: `/Users/sdevisch/repos/pandas_or_pyspark/data/example_small.csv`
- Ran at: 2025-09-06 07:51:58
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- Code file: `examples.py`
- System available cores: 11
- Args: assign=True, query='a >= 0', groupby='cat'

## Backend availability

| backend | version | status |
|---|---|---|
| pandas | 2.2.2 | available |
| dask | 2024.5.1 | available |
| pyspark | 3.5.1 | available |

## Results (seconds)

```text
backend  version   load_s  compute_s  rows  used_cores
-------  --------  ------  ---------  ----  ----------
pandas   2.2.2     0.0008     0.0000     2           1
dask     2024.5.1  0.0033     0.0081     2          11
pyspark  3.5.1     3.7435     0.4470     2          11
```
