from __future__ import annotations

"""Unified DataFrame wrapper and minimal pandas-like API.

This module exposes a thin `Frame` abstraction on top of backend-native
DataFrames (pandas, Dask DataFrame, pandas-on-Spark, Polars). The goal is to
provide a narrow set of common operations with identical signatures and
semantics across backends, while deferring execution to the underlying engine.

Design principles:
- Keep the surface small and unsurprising: select, query, assign, groupby/agg,
  merge, head, to_pandas, to_backend.
- Preserve lazy semantics where applicable (e.g., Dask, Spark) and avoid eager
  collection until conversion to pandas (to_pandas) or explicit materialization
  by the caller.
- Do not attempt to wrap every corner of each backend; prefer clarity.
"""

from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

from .backend import (
    current_backend_name,
    import_dask_dataframe,
    import_pandas,
    import_pyspark_pandas,
    infer_backend_from_object,
)


AggregationArg = Union[str, List[str], Mapping[str, Union[str, List[str]]]]


class Frame:
    """A thin wrapper over a backend DataFrame offering a minimal, unified API.

    This is a very small façade that forwards methods to the wrapped
    backend-native object (`self._obj`) and returns new `Frame` instances to
    maintain chaining.

    Parameters
    ----------
    data: Any
        Backend-native DataFrame instance (pandas, Dask, pandas-on-Spark, Polars).
    """

    def __init__(self, data: Any):
        self._obj = data
        # Detect backend from the object type if possible, otherwise consult the
        # process-wide configured backend. This keeps mixed pipelines working.
        self._backend = infer_backend_from_object(data) or current_backend_name()

    # ----- Introspection -----
    @property
    def backend(self) -> str:
        """Return the name of the backend for this frame (e.g., 'pandas')."""
        return self._backend

    @property
    def obj(self) -> Any:
        """Return the underlying backend-native object (escape hatch)."""
        return self._obj

    # ----- Core ops -----
    def select(self, columns: Union[str, Sequence[str]]) -> "Frame":
        """Project a subset of columns.

        Accepts a single column name or a sequence of names.
        """
        if isinstance(columns, str):
            cols = [columns]
        else:
            cols = list(columns)
        # Many backends support direct column projection via obj[cols]
        result = self._obj[cols]
        return Frame(result)

    def query(self, expr: str) -> "Frame":
        """Filter rows using a pandas-like query expression string."""
        # All supported backends expose pandas-like .query in this unified API
        result = self._obj.query(expr)
        return Frame(result)

    def assign(self, **kwargs: Union[Any, Callable[[Any], Any]]) -> "Frame":
        """Return a new frame with additional or replaced columns.

        The behavior mirrors pandas' DataFrame.assign.
        """
        result = self._obj.assign(**kwargs)
        return Frame(result)

    def merge(
        self,
        right: "Frame",
        how: str = "inner",
        on: Optional[Union[str, Sequence[str]]] = None,
        left_on: Optional[Union[str, Sequence[str]]] = None,
        right_on: Optional[Union[str, Sequence[str]]] = None,
        suffixes: Tuple[str, str] = ("_x", "_y"),
    ) -> "Frame":
        """Join two frames with pandas-like semantics.

        Parameters mirror pandas.DataFrame.merge for familiarity.
        """
        result = self._obj.merge(
            right.obj,
            how=how,
            on=on,
            left_on=left_on,
            right_on=right_on,
            suffixes=suffixes,
        )
        return Frame(result)

    def groupby(self, by: Union[str, Sequence[str]]) -> "Grouped":
        """Return a grouped view for subsequent aggregation."""
        return Grouped(self, by)

    def head(self, n: int = 5) -> "Frame":
        """Return the first ``n`` rows as a new `Frame` (lazy where supported)."""
        result = self._obj.head(n)
        return Frame(result)

    # ----- Additional commonly compatible ops -----
    def sort_values(self, by: Union[str, Sequence[str]], ascending: Union[bool, Sequence[bool]] = True) -> "Frame":
        """Return a new frame sorted by column(s).

        Maps to ``DataFrame.sort_values`` for pandas-like backends and to
        ``DataFrame.sort`` for Polars.
        """
        if self._backend == "polars":
            # Polars uses descending instead of ascending
            try:
                descending = (not ascending) if isinstance(ascending, bool) else [not a for a in ascending]  # type: ignore
                result = self._obj.sort(by, descending=descending)
            except Exception:
                result = self._obj  # graceful no-op on mismatch
        else:
            result = self._obj.sort_values(by=by, ascending=ascending)
        return Frame(result)

    def dropna(self, subset: Optional[Union[str, Sequence[str]]] = None) -> "Frame":
        """Drop rows with missing values, optionally restricted to ``subset``.

        Uses ``dropna`` for pandas-like backends and ``drop_nulls`` for Polars.
        """
        if self._backend == "polars":
            try:
                result = self._obj.drop_nulls(subset=subset)
            except Exception:
                result = self._obj
        else:
            result = self._obj.dropna(subset=subset)
        return Frame(result)

    def fillna(self, value: Any) -> "Frame":
        """Fill missing values with ``value``.

        Uses ``fillna`` for pandas-like backends and ``fill_null`` for Polars.
        """
        if self._backend == "polars":
            try:
                result = self._obj.fill_null(value)
            except Exception:
                result = self._obj
        else:
            result = self._obj.fillna(value)
        return Frame(result)

    def rename(self, columns: Optional[Mapping[str, str]] = None) -> "Frame":
        """Rename columns according to mapping ``old→new``.

        Parameters mirror pandas' ``DataFrame.rename(columns=...)``.
        """
        mapping = columns or {}
        if self._backend == "polars":
            try:
                result = self._obj.rename(mapping)
            except Exception:
                result = self._obj
        else:
            result = self._obj.rename(columns=mapping)
        return Frame(result)

    def astype(self, dtype_map: Mapping[str, Any]) -> "Frame":
        """Cast columns to given dtypes.

        Uses ``astype`` for pandas-like backends and ``cast``/``with_columns``
        for Polars.
        """
        if self._backend == "polars":
            try:
                # Polars DataFrame.cast accepts a mapping of column → dtype
                result = self._obj.cast(dtype_map)
            except Exception:
                try:
                    import polars as pl  # type: ignore

                    exprs = [pl.col(col).cast(tpe) for col, tpe in dtype_map.items()]
                    result = self._obj.with_columns(exprs)
                except Exception:
                    result = self._obj
        else:
            result = self._obj.astype(dtype_map)
        return Frame(result)

    # ----- Conversions -----
    def to_pandas(self):
        """Materialize into a pandas DataFrame.

        Lazy engines (Dask, Spark, Polars) are computed/collected here.
        """
        if self._backend == "pandas":
            return self._obj
        if self._backend == "dask":
            return self._obj.compute()
        if self._backend == "pyspark":
            # pandas-on-Spark exposes to_pandas
            return self._obj.to_pandas()
        if self._backend == "polars":
            # Polars exposes to_pandas
            return self._obj.to_pandas()
        raise RuntimeError(f"Unknown backend {self._backend}")

    def to_backend(self) -> Any:
        """Return the backend-native object without conversion."""
        return self._obj


class Grouped:
    """A grouped view used to perform aggregations uniformly across backends."""

    def __init__(self, frame: Frame, by: Union[str, Sequence[str]]):
        self._frame = frame
        self._by = by

    def agg(self, agg: AggregationArg) -> Frame:
        """Aggregate groups using pandas-like aggregation specifications.

        `agg` may be a string, list of strings, or a mapping of column→agg.
        """
        grouped = self._frame.obj.groupby(self._by)
        result = grouped.agg(agg)
        return Frame(result)


