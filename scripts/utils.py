from __future__ import annotations

import os
from typing import Dict, List, Optional


Backends = ["pandas", "dask", "pyspark"]


def get_backend_version(backend: str) -> Optional[str]:
    try:
        if backend == "pandas":
            import pandas as pd  # type: ignore

            return getattr(pd, "__version__", None)
        if backend == "dask":
            import dask  # type: ignore

            return getattr(dask, "__version__", None)
        if backend == "pyspark":
            import pyspark  # type: ignore

            return getattr(pyspark, "__version__", None)
    except Exception:
        return None
    return None


def used_cores_for_backend(backend: str) -> Optional[int]:
    try:
        if backend == "pandas":
            return 1
        if backend == "dask":
            try:
                from distributed import get_client  # type: ignore

                try:
                    client = get_client()
                    nthreads = getattr(client, "nthreads", None)
                    if isinstance(nthreads, dict):
                        return sum(int(v) for v in nthreads.values())
                except Exception:
                    pass
            except Exception:
                pass
            return os.cpu_count() or None
        if backend == "pyspark":
            try:
                from pyspark.sql import SparkSession  # type: ignore

                active = SparkSession.getActiveSession()
                if active is not None:
                    sc = active.sparkContext
                    return int(getattr(sc, "defaultParallelism", None) or 0) or None
            except Exception:
                pass
            return None
    except Exception:
        return None
    return None


def check_available(backend: str) -> bool:
    try:
        if backend == "pandas":
            __import__("pandas")
        elif backend == "dask":
            __import__("dask.dataframe")
        elif backend == "pyspark":
            __import__("pyspark.pandas")
        else:
            return False
        return True
    except Exception:
        return False


def format_fixed(headers: List[str], rows: List[List[str]], right_align_from: int = 2) -> List[str]:
    widths = [max(len(headers[i]), max((len(r[i]) for r in rows), default=0)) for i in range(len(headers))]

    def fmt_row(vals: List[str]) -> str:
        parts: List[str] = []
        for i, v in enumerate(vals):
            if i < right_align_from:
                parts.append(v.ljust(widths[i]))
            else:
                parts.append(v.rjust(widths[i]))
        return "  ".join(parts)

    lines = [fmt_row(headers), "  ".join(("-" * w) for w in widths)]
    for r in rows:
        lines.append(fmt_row(r))
    return lines
