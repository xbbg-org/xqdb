from __future__ import annotations

from datetime import date, datetime, time, timedelta
from functools import cache
from typing import Any

import narwhals as nw

from xqdb.xqdb import ArrowSeries, ArrowTable

_MAX_CONVERSION_DEPTH = 64
_SUPPORTED_OUTPUT_BACKENDS = frozenset(
    {
        nw.Implementation.PANDAS,
        nw.Implementation.PYARROW,
        nw.Implementation.POLARS,
    }
)


class _SeriesInput:
    __slots__ = ("_frame",)
    __xqdb_series__ = True

    def __init__(self, series: nw.Series) -> None:
        self._frame = series.to_frame()

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        return self._frame.__arrow_c_stream__(requested_schema=requested_schema)


def _as_eager_frame(value: object) -> nw.DataFrame | nw.Series | None:
    converted = nw.from_native(
        value,
        pass_through=True,
        eager_only=True,
        allow_series=True,
    )
    if isinstance(converted, nw.LazyFrame):
        raise TypeError("lazy dataframe inputs are not supported")
    if isinstance(converted, (nw.DataFrame, nw.Series)):
        return converted
    if converted is value:
        unrestricted = nw.from_native(
            value,
            pass_through=True,
            allow_series=True,
        )
        if isinstance(unrestricted, nw.LazyFrame):
            raise TypeError("lazy dataframe inputs are not supported")
    return None


def to_arrow_inputs(value: Any) -> Any:
    return _to_arrow_inputs(value, 0, set())


def _to_arrow_inputs(value: Any, depth: int, active: set[int]) -> Any:
    if depth > _MAX_CONVERSION_DEPTH:
        raise ValueError(f"Python value nesting exceeds {_MAX_CONVERSION_DEPTH} levels")

    if isinstance(
        value,
        (type(None), bool, int, float, str, bytes, date, datetime, time, timedelta),
    ):
        return value

    if isinstance(value, dict):
        identity = id(value)
        if identity in active:
            raise ValueError("cyclic Python containers cannot be converted to q")
        active.add(identity)
        try:
            return {
                key: _to_arrow_inputs(item, depth + 1, active)
                for key, item in value.items()
            }
        finally:
            active.remove(identity)

    if isinstance(value, list):
        identity = id(value)
        if identity in active:
            raise ValueError("cyclic Python containers cannot be converted to q")
        active.add(identity)
        try:
            return [_to_arrow_inputs(item, depth + 1, active) for item in value]
        finally:
            active.remove(identity)

    if isinstance(value, tuple):
        identity = id(value)
        if identity in active:
            raise ValueError("cyclic Python containers cannot be converted to q")
        active.add(identity)
        try:
            return tuple(_to_arrow_inputs(item, depth + 1, active) for item in value)
        finally:
            active.remove(identity)

    frame = _as_eager_frame(value)
    if isinstance(frame, nw.Series):
        return _SeriesInput(frame)
    if isinstance(frame, nw.DataFrame):
        return frame
    return value


@cache
def validate_backend(backend: str) -> str:
    implementation = nw.Implementation.from_backend(backend)
    if implementation not in _SUPPORTED_OUTPUT_BACKENDS:
        supported = ", ".join(sorted(item.value for item in _SUPPORTED_OUTPUT_BACKENDS))
        raise ValueError(
            f"unsupported dataframe backend: {backend!r}; expected one of {supported}"
        )
    implementation.to_native_namespace()
    return backend


def _from_arrow(value: object, backend: str) -> nw.DataFrame:
    return nw.from_arrow(value, backend=validate_backend(backend))


def from_arrow_results(value: Any, backend: str) -> Any:
    if isinstance(value, ArrowTable):
        return _from_arrow(value, backend)
    if isinstance(value, ArrowSeries):
        frame = _from_arrow(value, backend)
        return frame.get_column(value.name)
    if isinstance(value, tuple):
        return tuple(from_arrow_results(item, backend) for item in value)
    if isinstance(value, list):
        return [from_arrow_results(item, backend) for item in value]
    if isinstance(value, dict):
        return {key: from_arrow_results(item, backend) for key, item in value.items()}
    return value
