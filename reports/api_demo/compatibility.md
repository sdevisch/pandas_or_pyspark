# Compatibility matrix

Generated at: 2025-09-21 12:12:06

Generated at: 2025-09-21 12:12:06
- Python: `3.9.6` on `macOS-15.6.1-arm64-arm-64bit`
- CPU cores: 11
- RAM: 18.0 GiB

```text
operation    pandas         dask      pyspark       polars       duckdb  numpy  numba
-----------  ------  -----------  -----------  -----------  -----------  -----  -----
select           ok  unavailable  unavailable  unavailable  unavailable     ok     ok
query            ok  unavailable  unavailable  unavailable  unavailable     ok     ok
assign           ok  unavailable  unavailable  unavailable  unavailable     ok     ok
groupby_agg      ok  unavailable  unavailable  unavailable  unavailable   fail     ok
merge            ok  unavailable  unavailable  unavailable  unavailable     ok     ok
sort_values      ok  unavailable  unavailable  unavailable  unavailable     ok     ok
dropna           ok  unavailable  unavailable  unavailable  unavailable     ok     ok
fillna           ok  unavailable  unavailable  unavailable  unavailable     ok     ok
rename           ok  unavailable  unavailable  unavailable  unavailable     ok     ok
astype           ok  unavailable  unavailable  unavailable  unavailable     ok     ok
```
