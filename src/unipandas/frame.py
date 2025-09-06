from __future__ import annotations

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

    It delegates to pandas, Dask DataFrame, or pandas-on-Spark based on the
    current backend selection.
    """

    def __init__(self, data: Any):
        self._obj = data
        self._backend = infer_backend_from_object(data) or current_backend_name()

    # ----- Introspection -----
    @property
    def backend(self) -> str:
        return self._backend

    @property
    def obj(self) -> Any:
        return self._obj

    # ----- Core ops -----
    def select(self, columns: Union[str, Sequence[str]]) -> "Frame":
        if isinstance(columns, str):
            cols = [columns]
        else:
            cols = list(columns)
        result = self._obj[cols]
        return Frame(result)

    def query(self, expr: str) -> "Frame":
        # All three backends expose .query; dask requires meta sometimes but for simple use it works.
        result = self._obj.query(expr)
        return Frame(result)

    def assign(self, **kwargs: Union[Any, Callable[[Any], Any]]) -> "Frame":
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
        return Grouped(self, by)

    def head(self, n: int = 5) -> "Frame":
        result = self._obj.head(n)
        return Frame(result)

    # ----- Conversions -----
    def to_pandas(self):
        if self._backend == "pandas":
            return self._obj
        if self._backend == "dask":
            return self._obj.compute()
        if self._backend == "pyspark":
            # pandas-on-Spark exposes to_pandas
            return self._obj.to_pandas()
        raise RuntimeError(f"Unknown backend {self._backend}")

    def to_backend(self) -> Any:
        return self._obj


class Grouped:
    def __init__(self, frame: Frame, by: Union[str, Sequence[str]]):
        self._frame = frame
        self._by = by

    def agg(self, agg: AggregationArg) -> Frame:
        grouped = self._frame.obj.groupby(self._by)
        result = grouped.agg(agg)
        return Frame(result)


