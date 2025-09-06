# unipandas benchmark

## Run context

- Data file: `data/example.csv`
- Ran at: 2025-09-06 07:38:04
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- Code file: `examples.py`
- System available cores: 11
- Args: assign=True, query='a > 0', groupby='cat'

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
pandas   2.2.2     0.0212     0.0000     3           1
dask     2024.5.1  0.0053     0.0361     3          11
pyspark  3.5.1     3.9483     0.5864     3          11
```
