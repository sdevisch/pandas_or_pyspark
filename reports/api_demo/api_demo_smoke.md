# unipandas benchmark

## Run context

- Data file: `/Users/sdevisch/repos/pandas_or_pyspark/data/smoke.csv`
- Ran at: 2025-09-08 16:21:25
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
backend  version   load_s  compute_s  input_rows  used_cores
-------  --------  ------  ---------  ----------  ----------
pandas   2.2.2     0.0025     0.0000           3           1
dask     2024.5.1  0.0077     0.0211           3          11
pyspark  3.5.1     4.2984     0.7665           3          11
polars   1.33.0    0.0149     0.0000           3          11
duckdb   1.3.2     0.0467     0.0000           3          11
```
