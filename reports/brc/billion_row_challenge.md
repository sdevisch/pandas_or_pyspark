# Billion Row Challenge (scaffold)

Generated at: 2025-09-07 16:16:55

- operation: groupby
- materialize: count
- num_chunks: 1
- total_bytes: 73052357
- source: parquet
- input_rows: 10000000

```text
backend  version       op  read_s  compute_s  rows  used_cores
-------  -------  -------  ------  ---------  ----  ----------
pyspark  3.5.1    groupby  3.1573     1.4301     3          11
```

Groupby result preview:

```text
cat  x                            y
---  --------  --------------------
x    -1119005  -0.27007828729891425
z    119800     -0.2321410027150538
y    -1041988   0.22006234043638306
```
