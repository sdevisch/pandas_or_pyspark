# src/

This directory holds the installable `unipandas` library:

- `unipandas/backend.py`: Backend detection and configuration
- `unipandas/frame.py`: Thin `Frame` wrapper exposing a unified subset of pandas-like ops
- `unipandas/io.py`: IO functions (`read_csv`, `read_parquet`) returning `Frame`

Use `scripts/` for runnable tools; keep importable code here.
