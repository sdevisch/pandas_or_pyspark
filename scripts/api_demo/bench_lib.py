from __future__ import annotations

"""Shared helpers for bench_backends: small, readable, testable functions."""

import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

from unipandas import configure_backend, read_csv

# Import utils whether run as module or script
try:
    from .utils import (
        Backends as ALL_BACKENDS,
        get_backend_version as utils_get_backend_version,
        check_available as utils_check_available,
        used_cores_for_backend as utils_used_cores_for_backend,
    )
except Exception:  # script mode
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from utils import (  # type: ignore
        Backends as ALL_BACKENDS,
        get_backend_version as utils_get_backend_version,
        check_available as utils_check_available,
        used_cores_for_backend as utils_used_cores_for_backend,
    )


Backends = ALL_BACKENDS

# perfcore IO (optional)
_ROOT = Path(__file__).resolve().parents[2]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))
try:
    from perfcore.result import Result as PerfResult, write_results as perf_write_results  # type: ignore
except Exception:
    PerfResult = None  # type: ignore
    perf_write_results = None  # type: ignore


def _used_cores_for_backend(backend: str) -> Optional[int]:
    try:
        return utils_used_cores_for_backend(backend)
    except Exception:
        return None


def _count_rows_backend(df) -> int:
    try:
        import pandas as _pd  # type: ignore
        if isinstance(df, _pd.DataFrame):
            return int(len(df.index))
    except Exception:
        pass
    try:
        from dask.dataframe import DataFrame as _DaskDF  # type: ignore
        if isinstance(df, _DaskDF):
            return int(df.shape[0].compute())
    except Exception:
        pass
    try:
        from pyspark.pandas.frame import DataFrame as _PsDF  # type: ignore
        if isinstance(df, _PsDF):
            return int(df.to_spark().count())
    except Exception:
        pass
    try:
        import polars as _pl  # type: ignore
        if isinstance(df, _pl.DataFrame):
            return int(df.height)
    except Exception:
        pass
    try:
        return int(len(df))
    except Exception:
        return 0


def get_backend_version(backend: str) -> Optional[str]:
    return utils_get_backend_version(backend)


def check_available(backend: str) -> bool:
    return utils_check_available(backend)


def try_configure(backend: str) -> bool:
    try:
        if not check_available(backend):
            return False
        configure_backend(backend)
        return True
    except Exception:
        return False


def _read_input(path: str):
    t0 = time.perf_counter()
    df = read_csv(path)
    return df, time.perf_counter() - t0


def _apply_pipeline(df, assign: bool, query: Optional[str], groupby: Optional[str]):
    out = df
    if assign:
        try:
            out = out.assign(c=lambda x: x["a"] + x["b"])  # type: ignore
        except Exception:
            pass
    if query:
        try:
            out = out.query(query)
        except Exception:
            pass
    groups = None
    if groupby:
        try:
            out = out.groupby(groupby).agg({groupby: "count"})
            groups = _count_rows_backend(out.to_backend()) if hasattr(out, "to_backend") else None
        except Exception:
            groups = None
    return out, groups


def _materialize_and_time(df, rows: int) -> float:
    t0 = time.perf_counter()
    _ = df.head(rows).to_pandas()
    return time.perf_counter() - t0


def _build_result(backend: str, read_s: float, compute_s: float, input_rows: Optional[int], groups: Optional[int]):
    used = _used_cores_for_backend(backend)
    if PerfResult is None:
        class _Tmp:  # type: ignore
            def __init__(self, **kw):
                self.__dict__.update(kw)
        return _Tmp(
            frontend="unipandas",
            backend=backend,
            operation="bench",
            dataset_rows=None,
            input_rows=input_rows,
            read_seconds=read_s,
            compute_seconds=compute_s,
            groups=groups,
            used_cores=used,
            ok=True,
        )
    return PerfResult.now(
        frontend="unipandas",
        backend=backend,
        operation="bench",
        dataset_rows=None,
        input_rows=input_rows,
        read_seconds=read_s,
        compute_seconds=compute_s,
        groups=groups,
        used_cores=used,
        ok=True,
        notes=None,
    )


def _run_one_backend(backend: str, path: str, query: Optional[str], assign: bool, groupby: Optional[str], materialize_rows: int):
    if not try_configure(backend):
        return None
    df, read_s = _read_input(path)
    out, groups = _apply_pipeline(df, assign, query, groupby)
    compute_s = _materialize_and_time(out, materialize_rows)
    input_rows = _count_rows_backend(df.to_backend()) if hasattr(df, "to_backend") else None
    return _build_result(backend, read_s, compute_s, input_rows, groups)


def run_backends(path: str, query: Optional[str], assign: bool, groupby: Optional[str], materialize_rows: int) -> List["PerfResult"]:
    results: List["PerfResult"] = []
    for backend in Backends:
        r = _run_one_backend(backend, path, query, assign, groupby, materialize_rows)
        if r is not None:
            results.append(r)
    return results


def build_availability() -> List[Dict[str, object]]:
    info: List[Dict[str, object]] = []
    for name in Backends:
        info.append({"backend": name, "version": get_backend_version(name), "available": check_available(name)})
    return info


def rows_for_console(results: List["PerfResult"], availability: List[Dict[str, object]]) -> List[List[str]]:
    by_backend = {getattr(r, "backend", None): r for r in results}
    rows: List[List[str]] = []
    for name in Backends:
        r = by_backend.get(name)
        if r:
            used = getattr(r, "used_cores", None) or os.cpu_count()
            ver = get_backend_version(name)
            load_s = getattr(r, "read_seconds", None)
            comp_s = getattr(r, "compute_seconds", None)
            rows.append([name, str(ver), f"{load_s:.4f}" if isinstance(load_s, float) else "-", f"{comp_s:.4f}" if isinstance(comp_s, float) else "-", str(getattr(r, "input_rows", "-")), str(used)])
        else:
            ver = next((a["version"] for a in availability if a["backend"] == name), None)
            rows.append([name, str(ver), "-", "-", "-", "-"])
    return rows


def log_results_jsonl(results: List["PerfResult"], jsonl_out: Optional[str]) -> None:
    if not jsonl_out:
        return
    path = Path(jsonl_out)
    path.parent.mkdir(parents=True, exist_ok=True)
    if PerfResult is not None and perf_write_results is not None:
        perf_write_results(path, results, append=False)
        return
    import json as _json
    with path.open("w") as f:
        for r in results:
            payload = {
                "frontend": getattr(r, "frontend", "unipandas"),
                "backend": getattr(r, "backend", None),
                "operation": getattr(r, "operation", "bench"),
                "input_rows": getattr(r, "input_rows", None),
                "read_seconds": getattr(r, "read_seconds", None),
                "compute_seconds": getattr(r, "compute_seconds", None),
                "groups": getattr(r, "groups", None),
                "used_cores": getattr(r, "used_cores", None),
                "ok": getattr(r, "ok", True),
            }
            f.write(_json.dumps(payload) + "\n")


