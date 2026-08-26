import importlib
import math
import os
from datetime import date, datetime, time, timedelta, timezone

import narwhals as nw
import pyarrow as pa
import pytest
from xqdb.xqdb import generate_j6_ipc_msg

import xqdb
from xqdb import (
    Q,
    XqdbError,
    XqdbIOError,
    XqdbQLambda,
    XqdbQOperator,
    read_binary6,
    serialize_as_ipc_bytes6,
)


def _chunked(values, dtype):
    return pa.chunked_array([pa.array(values, type=dtype)])


def _native_series(value):
    assert isinstance(value, nw.Series)
    assert value.implementation is nw.Implementation.PYARROW
    native = nw.to_native(value)
    if isinstance(native, pa.Array):
        native = pa.chunked_array([native])
    assert isinstance(native, pa.ChunkedArray)
    return native


def _assert_series(value, expected):
    assert _native_series(value).equals(expected)


def _native_table(value):
    assert isinstance(value, nw.DataFrame)
    assert value.implementation is nw.Implementation.PYARROW
    native = nw.to_native(value)
    assert isinstance(native, pa.Table)
    return native


def _assert_table(value, expected):
    assert _native_table(value).equals(expected)


def test_q_function_value_exports_validation_and_exact_frames():
    plus = XqdbQOperator.PLUS
    assert plus.name == "+"
    assert XqdbQOperator("+").name == "+"
    assert XqdbQOperator("+") == plus
    assert XqdbQOperator.__module__ == "xqdb"
    assert repr(plus) == 'XqdbQOperator("+")'
    with pytest.raises(AttributeError):
        plus.name = "-"

    root = XqdbQLambda("{x+y}")
    contextual = XqdbQLambda(" {x+y} ", "ctx")
    assert (root.source, root.context) == ("{x+y}", "")
    assert (contextual.source, contextual.context) == (" {x+y} ", "ctx")
    assert repr(root) == 'XqdbQLambda("{x+y}")'
    assert repr(contextual) == 'XqdbQLambda(" {x+y} ", "ctx")'
    with pytest.raises(AttributeError):
        root.source = "{x-y}"

    with pytest.raises(XqdbError, match="unsupported q primitive"):
        XqdbQOperator("plus")
    with pytest.raises(XqdbError, match="NUL"):
        XqdbQOperator("+\0")
    with pytest.raises(XqdbError, match="brace-delimited"):
        XqdbQLambda("x+y")
    with pytest.raises(XqdbError, match="NUL"):
        XqdbQLambda("{x\0+y}")

    assert serialize_as_ipc_bytes6("sync", False, plus) == bytes(
        [1, 1, 0, 0, 10, 0, 0, 0, 102, 1]
    )
    assert (
        serialize_as_ipc_bytes6("sync", False, root)
        == bytes([1, 1, 0, 0, 21, 0, 0, 0, 100, 0, 10, 0, 5, 0, 0, 0]) + b"{x+y}"
    )


def test_qtype_facade_is_removed():
    assert not hasattr(xqdb, "QType")
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("xqdb.type")


def test_python_container_conversion_rejects_cycles_but_allows_reuse():
    cyclic_list = []
    cyclic_list.append(cyclic_list)
    with pytest.raises(ValueError, match="cyclic Python containers"):
        serialize_as_ipc_bytes6("sync", False, cyclic_list)

    cyclic_dict = {}
    cyclic_dict["self"] = cyclic_dict
    with pytest.raises(ValueError, match="cyclic Python containers"):
        serialize_as_ipc_bytes6("sync", False, cyclic_dict)

    shared = [1]
    assert serialize_as_ipc_bytes6("sync", False, [shared, shared])
    assert serialize_as_ipc_bytes6("sync", False, (shared, shared))


def test_python_container_conversion_enforces_maximum_depth():
    value = 0
    for _ in range(64):
        value = [value]
    assert serialize_as_ipc_bytes6("sync", False, value)
    with pytest.raises(ValueError, match="nesting exceeds 64 levels"):
        serialize_as_ipc_bytes6("sync", False, [value])


def test_serialization_preserves_python_error_types_and_message_validation():
    with pytest.raises(OverflowError):
        serialize_as_ipc_bytes6("sync", False, 1 << 100)
    with pytest.raises(ValueError, match="msg_type must be 0, 1, or 2"):
        generate_j6_ipc_msg(3, False, 1 << 100)
    with pytest.raises(TypeError):
        serialize_as_ipc_bytes6("sync", False, {1: "not a symbol key"})
    with pytest.raises(ValueError, match="expected 'async'.*got 'invalid'"):
        serialize_as_ipc_bytes6("invalid", False, 1)


def test_exception_module_exports_names():
    from xqdb import exceptions

    assert exceptions.__all__ == ["XqdbAuthError", "XqdbError", "XqdbIOError"]


@pytest.mark.parametrize(
    "value,error",
    [
        (datetime(1700, 1, 1, tzinfo=timezone.utc), OverflowError),
        (timedelta(days=106_752), OverflowError),
        (time(0, 0, 0, 1), ValueError),
    ],
)
def test_serialization_rejects_unrepresentable_temporal_values(value, error):
    with pytest.raises(error):
        serialize_as_ipc_bytes6("sync", False, value)


def test_call_argument_limit_is_checked_before_conversion():
    client = Q("does-not-exist.invalid", 1800, user="test")
    with pytest.raises(TypeError, match="at most 8 arguments"):
        client.sync("", 1 << 100, *([0] * 8))


@pytest.mark.parametrize(
    "query,expected",
    [
        ("0b", False),
        ("1b", True),
        ("0Ng", "00000000-0000-0000-0000-000000000000"),
        ("0xFF", 255),
        ("0Nh", -32768),
        ("0Ni", -2147483648),
        ("0N", -9223372036854775808),
        ("9", 9),
        ('"J"', "J"),
        ('"JS"', "JS"),
        ("`", ""),
        ("`q", "q"),
        (
            "1969.12.31D12:00:00.123456",
            datetime(1969, 12, 31, 12, 0, 0, 123456, tzinfo=timezone.utc),
        ),
        ("0001.01.01", date(1, 1, 1)),
        ("9999.12.31", date(9999, 12, 31)),
        ("0D12:34:56.123456", timedelta(seconds=45296, microseconds=123456)),
        (
            "-0D00:00:00.000001",
            timedelta(days=-1, seconds=86399, microseconds=999999),
        ),
        ("12:34:56.789", time(12, 34, 56, 789000)),
        (
            "2023.11.11T12:34:56.789",
            datetime(2023, 11, 11, 12, 34, 56, 789000, tzinfo=timezone.utc),
        ),
    ],
)
def test_read_scalar_invariance(q, query, expected):
    assert q.sync(query) == expected


@pytest.mark.parametrize(
    "query",
    [
        "0Wp",
        "0Nd",
        "-0Wd",
        "0Wd",
        "1969.12.31D12:00:00.123456789",
        "0D12:34:56.123456789",
    ],
)
def test_read_scalar_rejects_unrepresentable_temporal_values(q, query):
    with pytest.raises((XqdbError, OverflowError, ValueError)):
        q.sync(query)


def test_read_char_vector_string_and_arbitrary_bytes(q):
    assert q.sync('"xqdb"') == "xqdb"
    value = b"\x00\x7f\x80\xff"
    assert q.sync("{x}", value) == value


def test_round_trip_q_operator_and_lambda_values(q):
    expression = "{[op;a;b] .[op;(a;b)]}"
    assert q.sync(expression, XqdbQOperator.PLUS, 1, 2) == 3
    assert q.sync(expression, XqdbQLambda("{x+y}"), 1, 2) == 3
    operator = q.sync("+")
    q_lambda = q.sync("{x+y}")
    assert isinstance(operator, XqdbQOperator)
    assert isinstance(q_lambda, XqdbQLambda)
    assert q.sync(expression, operator, 1, 2) == 3
    assert q.sync(expression, q_lambda, 1, 2) == 3


@pytest.mark.parametrize(
    "query,dtype,values",
    [
        ("10b", pa.bool_(), [True, False]),
        ("(,)0b", pa.bool_(), [False]),
        ("(,)0Ng", pa.binary_view(), [bytes(16)]),
        ("0x00FF", pa.uint8(), [0, 255]),
        ("0N -0W 9 0Wh", pa.int16(), [None, None, 9, None]),
        ("0N -0W 9 0Wi", pa.int32(), [None, None, 9, None]),
        ("0N -0W 9 0W", pa.int64(), [None, None, 9, None]),
        ("0n -0w 9 0we", pa.float32(), [None, -math.inf, 9.0, math.inf]),
        ("0n -0w 9 0w", pa.float64(), [None, -math.inf, 9.0, math.inf]),
        ('("";"string")', pa.string_view(), ["", "string"]),
        (
            "0N 2021.06.03D0 2021.06.03D12:34:56.123456789p",
            pa.timestamp("ns"),
            [None, 1622678400000000000, 1622723696123456789],
        ),
        ("0N 2022.05.30d", pa.date32(), [None, date(2022, 5, 30)]),
        (
            "0N 0D00 0D12:34:56.123456789n",
            pa.duration("ns"),
            [None, 0, 45296123456789],
        ),
        ("0N 00:00 12:34u", pa.time64("ns"), [None, 0, 45240000000000]),
        ("0N 00:00:00 12:34:56v", pa.time64("ns"), [None, 0, 45296000000000]),
        ("0n 00:00:00.000 12:34:56.789t", pa.time64("ns"), [None, 0, 45296789000000]),
        (
            "0n 2022.06.03T00:00:00.000 2022.06.03T12:34:56.789z",
            pa.timestamp("ns"),
            [None, 1654214400000000000, 1654259696789000000],
        ),
        ("(1 2;();3 4)", pa.large_list(pa.int64()), [[1, 2], [], [3, 4]]),
        ("()", pa.null(), []),
    ],
)
def test_read_vector_types_and_null_semantics(q, query, dtype, values):
    _assert_series(q.sync(query), _chunked(values, dtype))


def test_read_symbol_vector_is_dictionary_encoded(q):
    actual = _native_series(q.sync("``q`kdb"))
    assert pa.types.is_dictionary(actual.type)
    assert actual.to_pylist() == ["", "q", "kdb"]


def test_recursive_dataframe_results_and_series_table_distinction(q):
    table = pa.table({"value": pa.array([1, 2], type=pa.int64())})
    series = pa.chunked_array([[3, 4]], type=pa.int64())
    result = q.sync("{x}", {"table": table, "nested": [series, table]})
    _assert_table(result["table"], table)
    _assert_series(result["nested"][0], series)
    _assert_table(result["nested"][1], table)

    assert q.sync("{type x}", table) == 98
    assert q.sync("{type x}", series) == 7


def test_arrow_stream_capsule_is_owned_exactly_once(q):
    class ReusedStream:
        def __init__(self, capsule):
            self.capsule = capsule

        def __arrow_c_stream__(self, requested_schema=None):
            return self.capsule

    table = pa.table({"value": [1, 2]})
    stream = ReusedStream(table.__arrow_c_stream__())
    assert serialize_as_ipc_bytes6("sync", False, stream)
    with pytest.raises((TypeError, ValueError), match="unused|released"):
        serialize_as_ipc_bytes6("sync", False, stream)

    bridge = q.q.sync("([]v:1 2)")
    assert bridge.shape == (2, 1)
    assert bridge.columns == ["v"]
    assert "ArrowTable" in repr(bridge)
    with pytest.raises(ValueError, match="schema negotiation"):
        bridge.__arrow_c_stream__(object())
    exported = ReusedStream(bridge.__arrow_c_stream__())
    converted = nw.from_arrow(exported, backend="pyarrow")
    assert isinstance(nw.to_native(converted), pa.Table)
    with pytest.raises((TypeError, ValueError)):
        nw.from_arrow(exported, backend="pyarrow")

    series_bridge = q.q.sync("1 2")
    assert series_bridge.shape == (2,)
    assert isinstance(series_bridge.name, str)
    assert "ArrowSeries" in repr(series_bridge)
    with pytest.raises(ValueError, match="schema negotiation"):
        series_bridge.__arrow_c_stream__(object())


def test_arrow_stream_requires_capsule_and_struct_schema():
    class BadCapsule:
        def __arrow_c_stream__(self, requested_schema=None):
            return object()

    class ArrayStream:
        def __arrow_c_stream__(self, requested_schema=None):
            return pa.chunked_array([[1, 2]]).__arrow_c_stream__()

    with pytest.raises(TypeError, match="must return a PyCapsule"):
        serialize_as_ipc_bytes6("sync", False, BadCapsule())
    with pytest.raises(TypeError, match="schema must be a struct"):
        serialize_as_ipc_bytes6("sync", False, ArrayStream())


class RawArrowStream:
    def __init__(self, table):
        self.table = table

    def __arrow_c_stream__(self, requested_schema=None):
        return self.table.__arrow_c_stream__(requested_schema=requested_schema)


@pytest.mark.parametrize(
    ("dtype", "values"),
    [
        pytest.param(pa.float16(), [1.0], id="float16"),
        pytest.param(pa.decimal128(10, 2), [None], id="decimal128"),
        pytest.param(pa.list_(pa.float16()), [[1.0]], id="nested-float16"),
    ],
)
def test_unsupported_arrow_dtype_returns_conversion_error(dtype, values):
    frame = pa.table({"value": pa.array(values, type=dtype)})
    with pytest.raises(ValueError, match="unsupported Arrow datatype"):
        serialize_as_ipc_bytes6("sync", False, RawArrowStream(frame))


def test_arrow_import_strips_untrusted_top_level_polars_metadata():
    dtype = pa.dictionary(pa.int32(), pa.string())
    field = pa.field(
        "value",
        dtype,
        metadata={b"_PL_ENUM_VALUES2": b"malformed"},
    )
    frame = pa.Table.from_arrays(
        [pa.array(["a", "b"], type=dtype)],
        schema=pa.schema([field]),
    )

    assert serialize_as_ipc_bytes6("sync", False, RawArrowStream(frame))


def test_arrow_import_preserves_benign_nested_metadata():
    dtype = pa.list_(
        pa.field("item", pa.int64(), metadata={b"semantic": b"identifier"})
    )
    frame = pa.table({"value": pa.array([[1, 2]], type=dtype)})

    assert serialize_as_ipc_bytes6("sync", False, RawArrowStream(frame))


def test_write_multichunk_and_sliced_nested_tables(q):
    frame = pa.table(
        {
            "value": pa.chunked_array([[1, 2], [3, 4]], type=pa.int64()),
            "depth": pa.chunked_array(
                [[[1.0, 2.0], []], [[3.0], [4.0, 5.0]]],
                type=pa.list_(pa.float64()),
            ),
        }
    )
    expected = pa.table(
        {
            "value": pa.array([1, 2, 3, 4], type=pa.int64()),
            "depth": pa.array(
                [[1.0, 2.0], [], [3.0], [4.0, 5.0]],
                type=pa.large_list(pa.float64()),
            ),
        }
    )
    _assert_table(q.sync("{x}", frame), expected)

    sliced = pa.table(
        {"depth": pa.array([[0.0], [1.0, None], [2.0, 3.0], [4.0]])}
    ).slice(1, 2)
    expected_slice = pa.table(
        {
            "depth": pa.array(
                [[1.0, None], [2.0, 3.0]],
                type=pa.large_list(pa.float64()),
            )
        }
    )
    _assert_table(q.sync("{x}", sliced), expected_slice)


@pytest.mark.parametrize(
    "dtype", [pa.int16(), pa.int32(), pa.int64(), pa.float32(), pa.float64()]
)
def test_write_nested_numeric_lists(q, dtype):
    frame = pa.table(
        {"depth": pa.array([[1, None, 2], [], [3, 4, 5, 6]], type=pa.list_(dtype))}
    )
    expected = pa.table(
        {
            "depth": pa.array(
                [[1, None, 2], [], [3, 4, 5, 6]],
                type=pa.large_list(dtype),
            )
        }
    )
    _assert_table(q.sync("{x}", frame), expected)


@pytest.mark.parametrize(
    "dtype,values,expected",
    [
        (pa.bool_(), [[True, None, False], []], [[True, False, False], []]),
        (pa.uint8(), [[1, None, 2], []], [[1, 0, 2], []]),
    ],
)
def test_write_nested_bool_and_byte_nulls(q, dtype, values, expected):
    frame = pa.table({"depth": pa.array(values, type=pa.list_(dtype))})
    expected_frame = pa.table({"depth": pa.array(expected, type=pa.large_list(dtype))})
    _assert_table(q.sync("{x}", frame), expected_frame)


def test_write_null_nested_containers_are_rejected(q):
    list_frame = pa.table(
        {"depth": pa.array([[1.0], None], type=pa.list_(pa.float64()))}
    )
    with pytest.raises(XqdbError, match="null values in List columns"):
        q.sync("{x}", list_frame)

    fixed_frame = pa.table(
        {"flags": pa.array([[True, False], None], type=pa.list_(pa.bool_(), 2))}
    )
    with pytest.raises(XqdbError, match="null values in Array columns"):
        q.sync("{x}", fixed_frame)


@pytest.mark.parametrize(
    "k_list,series",
    [
        ("10b", pa.chunked_array([[True, False]], type=pa.bool_())),
        ("0x00FF", pa.chunked_array([[0, 255]], type=pa.uint8())),
        ("0N -0W 9 0Wh", _chunked([None, -32767, 9, 32767], pa.int16())),
        ("0N -0W 9 0Wi", _chunked([None, -2147483647, 9, 2147483647], pa.int32())),
        (
            "0N -0W 9 0W",
            _chunked([None, -9223372036854775807, 9, 9223372036854775807], pa.int64()),
        ),
        ("0n -0w 9 0We", _chunked([math.nan, -math.inf, 9.0, math.inf], pa.float32())),
        ("0n -0w 9 0W", _chunked([math.nan, -math.inf, 9.0, math.inf], pa.float64())),
        ('("";"string")', _chunked(["", "string"], pa.string())),
        (
            "0N 2021.06.03D0 2021.06.03D12:34:56.123456789p",
            _chunked(
                [None, 1622678400000000000, 1622723696123456789], pa.timestamp("ns")
            ),
        ),
        ("0N 2022.05.30d", _chunked([None, date(2022, 5, 30)], pa.date32())),
        (
            "0N 0D00 0D12:34:56.123456789n",
            _chunked([None, 0, 45296123456789], pa.duration("ns")),
        ),
        (
            "0n 00:00:00.000 12:34:56.789t",
            _chunked([None, 0, 45296789000000], pa.time64("ns")),
        ),
        (
            "0n 2022.06.03T00:00:00.000 2022.06.03T12:34:56.789z",
            _chunked(
                [None, datetime(2022, 6, 3), datetime(2022, 6, 3, 12, 34, 56, 789000)],
                pa.timestamp("ms"),
            ),
        ),
    ],
)
def test_write_vector_types(q, k_list, series):
    assert q.sync("{x~" + k_list + "}", series)


def test_write_symbol_vector_from_dictionary_array(q):
    symbols = pa.chunked_array([pa.array(["", "q", "kdb"]).dictionary_encode()])
    assert q.sync("{x~``q`kdb}", symbols)


@pytest.mark.parametrize(
    "q_table,table",
    [
        (
            'enlist `float`long`char`string!(9.0;9;(,)"c";"string")',
            pa.table(
                {
                    "float": [9.0],
                    "long": pa.array([9], type=pa.int64()),
                    "char": ["c"],
                    "string": ["string"],
                }
            ),
        ),
        (
            'enlist `float`long`char`string!(0n;0N;(,)" ";"")',
            pa.table(
                {
                    "float": [math.nan],
                    "long": pa.array([None], type=pa.int64()),
                    "char": [" "],
                    "string": [""],
                }
            ),
        ),
        (
            "enlist `sym`timestamp`bool!(`sym;2021.06.03D;1b)",
            pa.table(
                {
                    "sym": pa.array(["sym"]).dictionary_encode(),
                    "timestamp": pa.array(
                        [1622678400000000000], type=pa.timestamp("ns")
                    ),
                    "bool": [True],
                }
            ),
        ),
    ],
)
def test_write_table_types(q, q_table, table):
    assert q.sync("{x~" + q_table + "}", table)


def test_read_table_returns_narwhals_with_arrow_schema(q):
    result = q.sync("([]sym:`a`b`c;prices:3 3#til 9)")
    native = _native_table(result)
    assert native.column_names == ["sym", "prices"]
    assert pa.types.is_dictionary(native.schema.field("sym").type)
    assert native["sym"].to_pylist() == ["a", "b", "c"]
    assert native["prices"].to_pylist() == [[0, 1, 2], [3, 4, 5], [6, 7, 8]]


def test_read_empty_table_preserves_schema(q):
    result = _native_table(q.sync("0#enlist `sym`timestamp`bool!(`sym;2022.06.05D;1b)"))
    assert result.num_rows == 0
    assert pa.types.is_dictionary(result.schema.field("sym").type)
    assert result.schema.field("timestamp").type == pa.timestamp("ns")
    assert result.schema.field("bool").type == pa.bool_()


def test_output_backend_is_exact_and_input_backend_is_independent(q):
    import pandas as pd
    import polars as pl

    inputs = {
        "pyarrow": pa.table({"value": [1, 2]}),
        "pandas": pd.DataFrame({"value": [1, 2]}),
        "polars": pl.DataFrame({"value": [1, 2]}),
    }
    native_types = {
        "pyarrow": pa.Table,
        "pandas": pd.DataFrame,
        "polars": pl.DataFrame,
    }
    original_backend = q.backend
    try:
        for output_backend, native_type in native_types.items():
            q.backend = output_backend
            for input_value in inputs.values():
                result = q.sync("{x}", input_value)
                assert isinstance(result, nw.DataFrame)
                assert result.implementation.value == output_backend
                assert isinstance(nw.to_native(result), native_type)
    finally:
        q.backend = original_backend


def test_series_backends_remain_series(q):
    import pandas as pd
    import polars as pl

    series_inputs = [
        pa.chunked_array([[1, 2]], type=pa.int64()),
        pd.Series([1, 2], name="value", dtype="int64"),
        pl.Series("value", [1, 2], dtype=pl.Int64),
    ]
    original_backend = q.backend
    try:
        for backend in ("pyarrow", "pandas", "polars"):
            q.backend = backend
            for series in series_inputs:
                result = q.sync("{x}", series)
                assert isinstance(result, nw.Series)
                assert result.implementation.value == backend
    finally:
        q.backend = original_backend


@pytest.mark.parametrize("backend", ["not-a-dataframe-backend", "duckdb", "modin"])
def test_unsupported_backend_raises_at_construction(backend):
    with pytest.raises(ValueError, match="unsupported dataframe backend"):
        Q("does-not-exist.invalid", 1800, backend=backend)


def test_unsupported_backend_assignment_does_not_fallback(q):
    original_backend = q.backend

    for backend in ("not-a-dataframe-backend", "duckdb", "modin"):
        with pytest.raises(ValueError, match="unsupported dataframe backend"):
            q.backend = backend
        assert q.backend == original_backend

    assert q.sync("1+1") == 2


def test_lazy_inputs_are_rejected_before_ipc(q):
    import polars as pl

    with pytest.raises(TypeError, match="lazy dataframe inputs"):
        q.sync("{x}", pl.LazyFrame({"value": [1, 2]}))


def test_logically_identical_backend_inputs_have_identical_ipc_bytes():
    import pandas as pd
    import polars as pl

    values = {
        "long": [1, 2, 3],
        "float": [1.5, None, 3.5],
        "string": ["a", "", "c"],
    }
    inputs = [pa.table(values), pd.DataFrame(values), pl.DataFrame(values)]
    encoded = [serialize_as_ipc_bytes6("sync", False, value) for value in inputs]
    assert encoded[0] == encoded[1] == encoded[2]


def test_read_binary6_honors_selected_backend(tmp_path):
    import pandas as pd
    import polars as pl

    path = tmp_path / "table.bin"
    ipc = serialize_as_ipc_bytes6("sync", False, pa.table({"v": [1, 2]}))
    path.write_bytes(b"\xff\x01" + ipc[8:])
    expected_types = {
        "pyarrow": pa.Table,
        "pandas": pd.DataFrame,
        "polars": pl.DataFrame,
    }
    for backend, expected_type in expected_types.items():
        result = read_binary6(str(path), backend=backend)
        assert isinstance(result, nw.DataFrame)
        assert result.implementation.value == backend
        assert isinstance(nw.to_native(result), expected_type)


def test_receive_uses_q_backend(q):
    import pandas as pd

    class Receiver:
        def __init__(self, value):
            self.value = value

        def receive(self):
            return self.value

    client = object.__new__(Q)
    client.retries = 0
    client.backend = "pandas"
    client.q = Receiver(q.q.sync("([]v:1 2)"))
    result = client.receive()
    assert isinstance(result, nw.DataFrame)
    assert result.implementation is nw.Implementation.PANDAS
    assert isinstance(nw.to_native(result), pd.DataFrame)


def test_asyn_accepts_arrow_backed_inputs(q):
    frame = pa.table({"value": [1, 2, 3]})
    assert q.asyn("{`xqdbTestX set count x}", frame) is None
    assert q.sync("xqdbTestX") == 3


def test_error_auto_connect_and_fixture(q):
    with pytest.raises(XqdbError, match="type"):
        q.sync("1+`a")
    with pytest.raises(XqdbError, match='"Not supported empty dictionary"'):
        q.sync('"()!()"', {})

    q.disconnect()
    assert q.sync("1+1") == 2
    q.connect()

    rows = int(os.environ.get("XQDB_Q_ROWS", "10000"))
    assert q.sync(".xqdb.ready")
    assert rows == q.sync("count trade")
    assert rows == q.sync("count wide")
    assert rows == q.sync("count depth")
    assert 14 == q.sync("count cols trade")
    assert 64 == q.sync("count cols wide")
    assert 5 == q.sync("count cols depth")


def test_io_error():
    q = Q("does-not-exist.invalid", 1800)
    with pytest.raises(XqdbIOError):
        q.sync("1+`a")
    with pytest.raises(XqdbIOError):
        q.asyn("1+`a")
