# Unified Markdown Reporting Plan

Generated: 2025-09-09

## Objective
Standardize how all scripts produce Markdown so that reports are consistent, easy to maintain, and testable. Consolidate on a single reporting layer that supports both fixed-width code blocks (current style) and optional GitHub pipe tables, with shared preface/suffix composition and system info.

## Current producers (inventory)
- BRC
  - `scripts/brc/billion_row_challenge.py` → uses `scripts/utils.write_fixed_markdown`
  - `scripts/brc/brc_one_minute_runner.py` → uses `scripts/utils.format_fixed`
  - `scripts/brc/brc_scale_runner.py` → uses `scripts/utils.format_fixed` (writes to `reports/`)
  - `scripts/brc/billion_row_om_runner.py` → orchestrator; should route through the same helpers
- API demos
  - `scripts/api_demo/bench_backends.py` → custom `_format_fixed_width_table`, writes file directly
  - `scripts/api_demo/compat_matrix.py` → custom `format_fixed_width(...)`, writes code block directly
  - `scripts/api_demo/relational_bench.py` → uses `scripts/utils.format_fixed`
  - `scripts/api_demo/smoke_reports.py` → orchestrates child scripts via `--md-out`
- Data utilities
  - `scripts/parquet_inventory.py` → custom `to_markdown(...)` (pipe table), writes directly
- Shared helpers
  - `scripts/utils.py` → `format_fixed(...)`, `write_fixed_markdown(...)`

## Options analysis
### A) Adopt an existing library
- Candidates: `tabulate` (direct), pandas `DataFrame.to_markdown()` (uses tabulate), `rich`
- Pros
  - Mature, well-tested formatting and alignment
  - Minimal code to maintain
- Cons
  - Less control over our “fixed-width code block” style
  - Harder to enforce consistent preface/suffix structure across all scripts
  - Still need shared composition (title/preface/system info/append) around the table

### B) Create a small internal package (recommended)
- Package: `mdreport` under `src/mdreport/`
- Pros
  - One standard API for title, preface, tables (fixed or pipe), suffix, and appending
  - Enforce consistent report root (`reports/`), system info block, and alignment defaults
  - Retain the current fixed-width style while optionally supporting pipe tables via `tabulate`
- Cons
  - Small amount of code to maintain

Recommendation: Build `mdreport` and optionally depend on `tabulate` only for pipe-table rendering. Keep our fixed-width formatting (current behavior) as first-class.

## Proposed `mdreport` API
- `Report(out_path: PathLike)` fluent builder
  - `.title(text: str)`
  - `.preface(lines: list[str])`  // e.g., operation, materialize, num_chunks, bytes, source, input_rows, system info
  - `.table(headers: list[str], rows: list[list[str]], align_from: int = 3, style: 'fixed'|'pipe' = 'fixed')`
  - `.suffix(lines: list[str])`   // e.g., groupby previews
  - `.write(append: bool = False)`
- Utilities
  - `system_info() -> list[str]` // Python, OS, CPU cores, optional RAM (psutil)
  - `strip_h1_from_fragment(text: str) -> str`
  - `resolve_report_path(path: PathLike) -> Path` // guarantees writing under repo-root `reports/`
  - `formatters`: float precision, NA as `-`, right-alignment from column N, truncation policies (optional)

## Style definitions
- `fixed`: code block with monospaced, right alignment from column N (current style)
- `pipe`: GitHub pipe table using `tabulate` with alignment and number formatting

## Migration plan (phases)
- Phase 0: Introduce `src/mdreport/`
  - Implement `Report`, `system_info`, `strip_h1_from_fragment`, `resolve_report_path`
  - Implement `fixed` style (replicate `scripts/utils.write_fixed_markdown` semantics)
  - Optional: add `pipe` style via `tabulate`
- Phase 1: Migrate producers
  - Keep `scripts/utils.write_fixed_markdown/format_fixed` as thin wrappers delegating to `mdreport` (backward compatible)
  - `scripts/brc/billion_row_challenge.py` → replace direct helper calls with `mdreport.Report`
  - `scripts/brc/brc_one_minute_runner.py`, `scripts/brc/brc_scale_runner.py`, `scripts/brc/billion_row_om_runner.py` → switch to `mdreport`
  - `scripts/api_demo/bench_backends.py` → drop `_format_fixed_width_table`, use `mdreport`
  - `scripts/api_demo/compat_matrix.py` → drop custom formatter, use `mdreport`
  - `scripts/api_demo/relational_bench.py` → use `mdreport`
  - `scripts/parquet_inventory.py` → switch to `mdreport` (likely `pipe` style)
- Phase 2: Tests
  - Snapshot tests for `fixed` and `pipe` styles (headers, alignment, NA handling)
  - Unit tests for dynamic columns (e.g., optional `groups`) and `align_from` behavior
  - End-to-end smoke tests: assert preface includes `source: parquet` and correct `input_rows`
- Phase 3: Documentation
  - Update `README.md` with a short example of `mdreport.Report`
  - Document report locations under root `reports/` and naming conventions
- Phase 4: Cleanup
  - Remove duplicated table-generation logic from scripts
  - Ensure no script writes under `scripts/reports/`

## Mapping (file → action)
- `scripts/brc/billion_row_challenge.py`: adopt `mdreport.Report`; dynamic `groups` column; groupby preview as suffix
- `scripts/brc/brc_one_minute_runner.py`: use `mdreport` for fixed-width table
- `scripts/brc/brc_scale_runner.py`: use `mdreport`; ensure root `reports/`
- `scripts/brc/billion_row_om_runner.py`: ensure it composes via `mdreport`
- `scripts/api_demo/bench_backends.py`: replace custom formatter with `mdreport`
- `scripts/api_demo/compat_matrix.py`: replace custom formatter with `mdreport`
- `scripts/api_demo/relational_bench.py`: use `mdreport`
- `scripts/parquet_inventory.py`: use `mdreport` with `pipe` style
- `scripts/utils.py`: wrap `mdreport` for compatibility; later, deprecate direct use

## Risks & mitigations
- Divergent formatting expectations → keep `fixed` style identical; add tests
- Unintended path writes → enforce `resolve_report_path`
- Extra dependency (`tabulate`) → optional; only needed for `pipe` style

## Timeline (rough)
- Day 1: Implement `mdreport` (fixed style), add tests
- Day 2: Migrate BRC + demos; update README; add pipe style (optional)
- Day 3: Migrate remaining utilities; cleanup and deprecations

## Decision
Proceed with Option B (internal `mdreport`) and maintain optional support for `pipe` tables via `tabulate`. This balances control, consistency, and low maintenance overhead.
