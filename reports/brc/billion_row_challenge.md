# Billion Row Challenge (scaffold)

Generated at: 2025-09-07 16:32:33

- operation: groupby
- materialize: count
- num_chunks: 10
- total_bytes: 1386812141
- source: parquet
- input_rows: 190000000

```text
backend  version       op  read_s  compute_s  rows  used_cores
-------  -------  -------  ------  ---------  ----  ----------
pyspark  3.5.1    groupby  3.9003     4.5092     3          11
```

Groupby result preview:

```text
cat  x                             y
---  --------  ---------------------
x    837206    -0.058281291202761404
z    2191161    -0.03414418368772246
y    -5680157    -0.1180479116831329
```
