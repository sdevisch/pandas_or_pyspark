# Performance-Reporting Separation and Front-End Expansion Plan

## Goals
- Separate performance data generation from Markdown reporting.
- Introduce pluggable front-end interfaces (pandas, PySpark, Narwhals) over pluggable backends.
- Store results in a small on-disk "results DB" (JSONL/Parquet) with a stable schema for later reporting.
- Keep BRC groupby-only by default at 1B rows; extend the same harness to front-end adapters.

## Proposed Architecture
- Core runner (library): `perfcore`
  - Function: `measure(frontend: str, backend: str, op: str, dataset_glob: str, materialize: str) -> Result`.
  - No I/O to Markdown. Emits `Result` records.
- Results store: `results/` directory
  - Format: JSON Lines for simplicity (`results/perf.jsonl`) and optional Parquet mirror (`results/perf.parquet`).
  - Writer API: `write_result(Result)` appends one record; `write_results(List[Result])` appends many.
- Reporting layer: `mdreport` usage only here
  - Reads from the results store, composes Markdown (full matrix, summaries, trends).
  - Multiple reports can be generated without re-running measurements.

## Result Schema (JSONL per record)
- `timestamp`: ISO string
- `frontend`: one of `pandas`, `pyspark`, `narwhals`
- `backend`: one of `pandas`, `dask`, `pyspark`, `polars`, `duckdb`, `numpy`, `numba`
- `operation`: `groupby`
- `dataset_rows`: integer (intended size)
- `input_rows`: integer (verified via parquet footer when available)
- `read_seconds`: float or null
- `compute_seconds`: float or null
- `groups`: integer or null
- `used_cores`: integer or null
- `python`: version string
- `platform`: platform string
- `ok`: boolean
- `notes`: optional string

## Front-End Adapters
- `frontend/pandas.py`: delegates to `unipandas` current API
- `frontend/pyspark.py`: sets `UNIPANDAS_BACKEND=pyspark` and applies Spark-friendly paths
- `frontend/narwhals.py`: shim that maps Narwhals DataFrame ops to `unipandas.Frame`

Each exposes: `load(glob) -> Frame`, `groupby_agg(Frame) -> Frame`, `count_rows(obj) -> int`.

## Milestones
1) Extract `perfcore` from current BRC code paths
   - Move timing, input row verification, and counting into reusable functions
   - Add JSONL writer (`results/perf.jsonl`)
2) Implement `pandas` and `pyspark` front-ends
   - Validate on tiny parquet and on 100M data
3) Add `narwhals` front-end stub
   - Initial ops limited to groupby-aggs
4) Reporting scripts
   - `scripts/reports/perf_matrix.py` reads JSONL and renders full matrix via `mdreport`
   - `scripts/reports/trends.py` for time-series of compute/read times
5) Wire BRC to use `perfcore`
   - `billion_row_challenge.py` calls `perfcore` and writes results; separate script composes Markdown

## Risks & Mitigations
- Mixed environment effects (Spark session): keep isolation per run; prefer subprocess for Spark
- Narwhals op coverage: start with minimal set; fallback to pandas path for missing ops
- Results growth: rotate/partition JSONL files (by date) and provide compaction to Parquet

## Acceptance
- Measurements can be re-rendered into Markdown without re-running compute
- New front-ends can be added by implementing a small adapter interface
- BRC report shows a complete matrix driven by data in the results store
