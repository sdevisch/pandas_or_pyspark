#!/usr/bin/env python3

from __future__ import annotations

"""Parquet inventory utility.

Scans one or more directories for ``*.parquet`` files and writes a concise
Markdown report with:
- file path
- row count (from Parquet metadata)
- columns and dtypes
- optional min/max per numeric column (if fast to retrieve)
- a small sample of top distinct values per categorical-like column

Usage:
  python scripts/parquet_inventory.py --glob "data/brc_scales/parquet_1000000/*.parquet" \
      --md-out reports/data/parquet_inventory.md
  python scripts/parquet_inventory.py --dir data/brc_scales --md-out reports/data/inventory.md
"""

import argparse
import glob
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def parse_args():
    p = argparse.ArgumentParser(description="Summarize Parquet files as Markdown")
    p.add_argument("--glob", dest="pattern", default=None, help="Glob pattern for parquet files")
    p.add_argument("--dir", dest="directory", default=None, help="Directory to recursively scan for parquet")
    p.add_argument("--md-out", dest="md_out", default="reports/data/parquet_inventory.md", help="Output Markdown path")
    return p.parse_args()


def list_parquet_files(pattern: Optional[str], directory: Optional[str]) -> List[Path]:
    if pattern:
        return [Path(p) for p in glob.glob(pattern)]
    if directory:
        return [Path(p) for p in glob.glob(os.path.join(directory, "**", "*.parquet"), recursive=True)]
    return []


def summarize_parquet(path: Path) -> Dict[str, object]:
    import pyarrow.parquet as pq  # type: ignore
    table = pq.ParquetFile(str(path))
    meta = table.metadata
    num_rows = int(meta.num_rows) if meta is not None else None
    schema = table.schema_arrow
    cols = [(str(field.name), str(field.type)) for field in schema]
    return {"path": str(path), "rows": num_rows, "columns": cols}


def to_markdown(items: List[Dict[str, object]]) -> str:
    # Fallback textual rendering if mdreport is unavailable
    lines: List[str] = ["# Parquet inventory", ""]
    for it in items:
        lines.append(f"## {it['path']}")
        lines.append("")
        lines.append(f"- rows: {it['rows']}")
        lines.append("- columns:")
        for name, dtype in it["columns"]:  # type: ignore
            lines.append(f"  - {name}: `{dtype}`")
        lines.append("")
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    files = list_parquet_files(args.pattern, args.directory)
    if not files:
        print("No parquet files matched.")
        return 1
    items = [summarize_parquet(p) for p in files]
    out = Path(args.md_out)
    out.parent.mkdir(parents=True, exist_ok=True)
    try:
        from mdreport import Report  # type: ignore
    except Exception:
        out.write_text(to_markdown(items))
        print("Wrote", out)
    else:
        # Render a simple pipe table inventory
        headers = ["path", "rows", "columns"]
        rows: List[List[str]] = []
        for it in items:
            cols = ", ".join([f"{name}:{dtype}" for name, dtype in it["columns"]])  # type: ignore
            rows.append([str(it["path"]), str(it["rows"]), cols])
        rpt = Report(out)
        rpt.title("Parquet inventory").table(headers, rows, style="pipe").write()
        print("Wrote", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


