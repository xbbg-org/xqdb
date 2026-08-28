# XQDB — Python Bindings

XQDB is independent and not affiliated with or endorsed by KX. kdb+ is a trademark of KX.

A Python interface to kdb+/q powered by Narwhals, with support for multiple dataframe backends (PyArrow, pandas, Polars).

## Installation

**Requirements**: Python ≥ 3.10, Narwhals ≥ 2.10, PyArrow ≥ 20.0.0

Optional backend packages: `pandas`, `polars`

Install the published package:

```bash
python -m pip install xqdb
```

To build from source for development with a Rust toolchain:

```bash
python -m pip install -e .
```

## Quick Start

```python
import xqdb
import narwhals as nw

# Basic connection (PyArrow backend by default)
conn = xqdb.Q('localhost', 1800)

# Select an installed Narwhals output backend
conn = xqdb.Q('localhost', 1800, backend='polars')

# Authentication credentials require TLS unless the connection is already protected
conn = xqdb.Q(
    'localhost', 1800, user='user', passwd='password', enable_tls=True
)

# With TLS and retry
conn = xqdb.Q('localhost', 1800, enable_tls=True, retries=3, timeout=30)
```

### Connection Parameters

| Parameter    | Type   | Default     | Description                                        |
| ------------ | ------ | ----------- | -------------------------------------------------- |
| `host`       | `str`  |             | Hostname of the q process                          |
| `port`       | `int`  |             | Port of the q process                              |
| `backend`    | `str`  | `"pyarrow"` | Installed Narwhals output backend; tested with `"pyarrow"`, `"pandas"`, and `"polars"` |
| `user`       | `str`  | `""`        | q username; empty unless explicitly supplied       |
| `passwd`     | `str`  | `""`        | Password                                           |
| `enable_tls` | `bool` | `False`     | Enable TLS with platform certificate verification |
| `retries`    | `int`  | `0`         | Number of retries with exponential backoff          |
| `timeout`    | `int`  | `0`         | Connection timeout in seconds (0 = no timeout)     |

q IPC authentication sends credentials in cleartext when TLS is disabled. Enable TLS for credentialed connections unless another trusted transport already protects the socket.

### Narwhals DataFrames and Backend Selection

Results from `conn.sync()` and `conn.receive()` return [Narwhals](https://narwhals-dev.github.io/narwhals/) DataFrames or Series backed by the selected backend. The default is PyArrow; pandas and Polars are supported when their optional packages are installed. An unavailable or unknown backend raises an error—XQDB does not silently substitute another backend.

To extract the underlying native DataFrame:

```python
result = conn.sync("select from trade")  # Narwhals DataFrame
native_df = nw.to_native(result)  # PyArrow, pandas, or Polars Table/DataFrame
```

### Input Constraints

- Native or Narwhals eager DataFrames and Series are accepted.
- Lazy frames are rejected rather than collected implicitly.
- The Python/native boundary uses the Arrow C Stream interface; it does not serialize frames to Arrow IPC bytes.

### Temporal range and precision

Python `datetime`, `time`, and `timedelta` values have microsecond precision. q timestamp or timespan atoms with non-zero sub-microsecond nanoseconds raise `ValueError` instead of being truncated. q date or datetime atoms outside Python's representable range raise `OverflowError` instead of being clamped.

A q timestamp carries no timezone, so it maps to a **naive** `datetime`. Timestamp atoms and Arrow `timestamp[ns]` columns therefore share the same timezone semantics, and for any value Python's `datetime` can represent they compare equal. XQDB does not label q values UTC, because a q process may hold local wall-clock times; apply your own zone when you know it.

Query arguments accept both shapes. A naive `datetime` is used as the q wall clock unchanged. An aware `datetime` is resolved to UTC by Python, so fixed offsets, `zoneinfo` zones, and other `tzinfo` implementations all normalize to the correct instant, including across DST boundaries.

Whole Series and DataFrame round-trips are always nanosecond-exact on every backend, because they cross the boundary as Arrow rather than as Python objects. **Single scalars pulled out of a frame are backend-dependent**, and the precision is decided by the backend before XQDB sees the value:

| Backend | Scalar type | Sub-microsecond digits |
| --- | --- | --- |
| `pyarrow` (pandas installed) | `pandas.Timestamp` | preserved via `.nanosecond` |
| `pyarrow` (no pandas) | — | PyArrow refuses to convert and raises |
| `pandas` | `pandas.Timestamp` | preserved via `.nanosecond` |
| `polars` | `datetime.datetime` | **truncated by Polars before XQDB sees it** |

`pandas.Timestamp` keeps its sub-microsecond digits in `.nanosecond` rather than `.microsecond`, and XQDB reads that remainder so the full nanosecond reaches q. Polars materializes a plain `datetime`, so a nanosecond read out as a Polars scalar is already truncated and XQDB cannot recover it. When nanosecond fidelity matters, pass the frame or Series instead of a scalar.

Reading a sub-microsecond value back as an *atom* still raises `ValueError`, because `datetime` cannot represent it — select it as a one-row table instead.

### Connect / Disconnect

```python
# explicitly connect (auto-connects on first query)
conn.connect()

# disconnect (auto-disconnects on IO error)
conn.disconnect()
```

### String Query

```python
conn.sync("select from trade where date=last date")
```

### Functional Query

Supports Python [basic data types](#basic-data-type), Narwhals Series/DataFrame, and `dict` (with string keys).

```python
from datetime import date, time

import pyarrow as pa

symbols = pa.chunked_array(
    [pa.array(["sym0", "sym1"]).dictionary_encode()]
)
conn.sync(
    ".gw.query",
    "table",
    {
        "date": date(2023, 11, 21),
        "syms": symbols,
        "startTime": time(9),
        "endTime": time(11, 30),
    },
)
```

### Operators and Lambdas

Pass q primitives and arbitrary lambdas as first-class arguments:

```python
from xqdb import XqdbQLambda, XqdbQOperator

conn.sync("{[op;a;b] .[op;(a;b)]}", XqdbQOperator.PLUS, 1, 2)
conn.sync("{[op;a;b] .[op;(a;b)]}", XqdbQLambda("{x+y}"), 1, 2)

# A non-root q context can be supplied explicitly.
scoped = XqdbQLambda("{x+y}", "analytics")
```

`XqdbQOperator(name)` accepts supported q primitive names such as `"+"`; it does not expose wire opcodes. `XqdbQLambda(source, context="")` preserves its source text, requires a brace-delimited UTF-8 body (optionally prefixed with `k)`), rejects NUL bytes in both fields, and rejects context values beginning with `"."`. The context `"analytics"` represents q namespace `.analytics` because the wire context omits the leading dot. Lambda source is executable q code: construct it only from trusted input.

### Send DataFrame

```python
import pyarrow as pa

frame = pa.table({"sym": ["a", "b"], "price": [10.5, 11.0]})
conn.sync("upsert", "table", frame)
```

### Async Query

```python
conn.asyn("upsert", "table", frame)
```

### Subscribe

```python
import pyarrow as pa

tables = pa.chunked_array(
    [pa.array(["table1", "table2"]).dictionary_encode()]
)
symbols = pa.chunked_array(
    [pa.array(["sym1", "sym2"]).dictionary_encode()]
)
conn.sync(".u.sub", tables, "")
conn.sync(".u.sub", tables, symbols)

while True:
    # returns ("upd", "table", Narwhals DataFrame)
    upd = conn.receive()
    print(upd)
```

### Generate IPC Bytes

Serialize data as kdb+ IPC bytes without a connection.

```python
import pyarrow as pa

from xqdb import serialize_as_ipc_bytes6

frame = pa.table({"sym": ["a", "b"], "price": [10.5, 11.0]})

# without compression
buffer = serialize_as_ipc_bytes6("sync", False, ["upd", "table", frame])

# with compression
buffer = serialize_as_ipc_bytes6("sync", True, ["upd", "table", frame])
```

**`msg_type`**: `"async"` | `"sync"` | `"response"`

### Read Binary Table

Read a regular q binary table file directly into a Narwhals DataFrame. Select the native output backend independently of the file format.

```python
from xqdb import read_binary6

df = read_binary6("/path/to/table.bin", backend="pandas")
```

## Error Handling

```python
from xqdb import XqdbError, XqdbIOError, XqdbAuthError

try:
    conn.sync("select from trade")
except XqdbAuthError:
    print("Authentication failed")
except XqdbIOError:
    print("Connection error")
except XqdbError:
    print("General xqdb error")
```

## Data Type Mapping

### Deserialization (q → Python)

q scalars map to Python scalars; q vectors/tables are returned as Narwhals DataFrames/Series backed by the selected backend.

#### Atom (scalar to Python)

| q type      | n   | size | Python type  | Note                        |
| ----------- | --- | ---- | ------------ | --------------------------- |
| `boolean`   | 1   | 1    | `bool`       |                             |
| `guid`      | 2   | 16   | `str`        |                             |
| `byte`      | 4   | 1    | `int`        |                             |
| `short`     | 5   | 2    | `int`        |                             |
| `int`       | 6   | 4    | `int`        |                             |
| `long`      | 7   | 8    | `int`        |                             |
| `real`      | 8   | 4    | `float`      |                             |
| `float`     | 9   | 8    | `float`      |                             |
| `char`      | 10  | 1    | `str`        |                             |
| `string`    | 10  | 1    | `str`        |                             |
| `symbol`    | 11  | \*   | `str`        |                             |
| `timestamp` | 12  | 8    | `datetime`   | naive; no timezone attached |
| `month`     | 13  | 4    | `-`          |                             |
| `date`      | 14  | 4    | `date`       | 0001.01.01 - 9999.12.31     |
| `datetime`  | 15  | 8    | `datetime`   | naive; no timezone attached |
| `timespan`  | 16  | 8    | `timedelta`  |                             |
| `minute`    | 17  | 4    | `time`       | 00:00 - 23:59               |
| `second`    | 18  | 4    | `time`       | 00:00:00 - 23:59:59         |
| `time`      | 19  | 4    | `time`       | 00:00:00.000 - 23:59:59.999 |
| `primitive` | 101-103 | 1   | `XqdbQOperator` | supported unary/binary/ternary primitive |
| `lambda`    | 100 | \*   | `XqdbQLambda` | source and q context |

#### Vector and Table (Arrow-backed Narwhals)

| q type           | PyArrow representation | Notes                              |
| ---------------- | ---------------------- | ---------------------------------- |
| `boolean list`   | `bool`                 | Native Arrow boolean               |
| `byte list`      | `uint8`                | Native Arrow unsigned 8-bit        |
| `short list`     | `int16`                | Native Arrow signed 16-bit         |
| `int list`       | `int32`                | Native Arrow signed 32-bit         |
| `long list`      | `int64`                | Native Arrow signed 64-bit         |
| `real list`      | `float32`              | Native Arrow single precision      |
| `float list`     | `float64`              | Native Arrow double precision      |
| `char`/strings   | `string_view`          | Arrow UTF-8 string view            |
| `symbol list`    | dictionary-encoded string | Preserves q symbol semantics    |
| `guid list`      | `binary_view`          | Every non-null value is 16 bytes   |
| nested list      | `large_list`           | Child type follows the q list type |
| `timestamp list` | `timestamp[ns]`        | Nanosecond timestamp               |
| `date list`      | `date32`               | Days since the Unix epoch          |
| `datetime list`  | `timestamp[ms]`        | Millisecond timestamp              |
| `timespan list`  | `duration[ns]`         | Nanosecond duration                |
| `minute list`    | `time64[ns]`           | Nanosecond time-of-day             |
| `second list`    | `time64[ns]`           | Nanosecond time-of-day             |
| `time list`      | `time64[ns]`           | Nanosecond time-of-day             |
| `table`          | `pyarrow.Table`        | Returned through a Narwhals frame  |
| `keyed table`    | `pyarrow.Table`        | Key and value columns are combined |

Other selected backends receive the equivalent representation that Narwhals can construct from this Arrow stream. Backend-specific dtypes may differ while values and q semantics remain the same.

> `real`/`float` `0n` is mapped to null, not `NaN`.

> `short`/`int`/`long` null and infinity values (`0Nh/i/j`, `0Wh/i/j`, `-0Wh/i/j`) are mapped to null.

### Serialization (Python → q)

#### Basic Data Type

| Python type  | q type      | Note                        |
| ------------ | ----------- | --------------------------- |
| `bool`       | `boolean`   |                             |
| `int`        | `long`      |                             |
| `float`      | `float`     |                             |
| `str`        | `symbol`    |                             |
| `bytes`      | `string`    |                             |
| `datetime`   | `timestamp` | naive used as-is; aware resolved to UTC |
| `date`       | `date`      | 0001.01.01 - 9999.12.31     |
| `timedelta`  | `timespan`  |                             |
| `time`       | `time`      | 00:00:00.000 - 23:59:59.999 |
| `XqdbQOperator` | primitive | supported primitive name |
| `XqdbQLambda` | lambda | source and q context |

#### Series, DataFrame, and Dictionary

| Arrow C Stream dtype | q type |
| -------------------- | ------ |
| boolean              | boolean list |
| uint8                | byte list |
| int16                | short list |
| int32                | int list |
| int64                | long list |
| float32              | real list |
| float64              | float list |
| string/string view   | general list of char vectors |
| dictionary-encoded string | symbol list |
| 16-byte binary values | guid list |
| timestamp            | timestamp list |
| date32               | date list |
| duration             | timespan list |
| time64               | time list |
| nested numeric list  | general list of typed q vectors |
| eager DataFrame      | table |

> Dictionary serialization requires `str` keys.

## Resources

- [Narwhals Documentation](https://narwhals-dev.github.io/narwhals/) — Unified dataframe interface
- [PyArrow Documentation](https://arrow.apache.org/docs/python/) — Default backend
- [Pandas Documentation](https://pandas.pydata.org/docs/) — Optional backend
- [Polars Documentation](https://docs.pola.rs/) — Optional backend

## License

XQDB is licensed under the [BSD-3-Clause](https://github.com/xbbg-org/xqdb/blob/main/LICENSE) permissive open-source license, which permits use in proprietary and commercial applications.
