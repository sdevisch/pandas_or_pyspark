# Repo cleanup plan

Date: 2025-09-10
Branch: chore/repo-cleanup

## Goals
- Enforce Parquet-only BRC path (no CSV fallbacks/helpers anywhere).
- Standardize all Markdown via `mdreport.Report` and remove duplicate formatters.
- Ensure all reports write under `reports/` at repo root.
- Simplify code by removing dead helpers and stale docstrings.

## Findings (quick scan)
- CSV remnants in BRC:
  - `scripts/brc/billion_row_om_runner.py`: `_csv_glob`, CSV conversion helpers, CSV source in `Entry`, and CSV branch in `_entry_for_backend_size`.
  - `scripts/brc/brc_shared.py`: falls back to `read_csv` for non-parquet suffix.
  - `scripts/brc/billion_row_challenge.py`: legacy CSV generation helpers (`_write_rows_csv`, `_maybe_generate_chunk`, `ensure_chunks`), and source detection references to CSV.
- Duplicate table formatters linger (now redundant with `mdreport`):
  - Local `fmt_fixed`/`format_fixed_width` variants in runner/demo scripts.
- Report paths:
  - Verify no writers still target `scripts/reports/**` (migrate to `reports/**`).
- System info:
  - Some scripts roll their own; prefer `mdreport.system_info()`.

## Proposed edits
1) BRC Parquet-only hardening
   - `scripts/brc/brc_shared.py`: remove CSV paths; require `.parquet` and raise on others.
   - `scripts/brc/billion_row_om_runner.py`: remove `_csv_glob`, CSV conversion, CSV counting; only Parquet glob; set `Entry.source="parquet"` when using existing data else `generated`.
   - `scripts/brc/billion_row_challenge.py`: delete CSV generation helpers and stale CLI/docs referring to CSV; keep `resolve_chunks` parquet-only; keep `_detect_source` returning `parquet`.
2) mdreport standardization
   - Drop local `_format_fixed_width_table`/`fmt_fixed`/`format_fixed_width` where still present; use `mdreport.Report` with fixed style.
   - Use `mdreport.system_info()` in prefaces where applicable.
3) Report locations
   - Confirm all writers target `reports/**`. Remove any references to `scripts/reports/**`. Delete orphaned files under `scripts/reports/` if present.
4) Docs/tests
   - README: remove CSV-centric examples for BRC; keep Parquet; add note about mdreport.
   - Tests: ensure they assume parquet-only for BRC; add a check that report prefaces show `source: parquet`.

## Validation
- Run unit tests.
- Run `demo_api_3_min` to ensure reports generate and aggregate correctly.
- Run BRC 1M quick check to verify headers (`source: parquet`, `input_rows` correct) and groupby preview.

## Nice-to-have (optional)
- Pre-commit config for formatting/lint.
- A small CLI in `mdreport` to render a CSV to markdown (pipe/fixed).
