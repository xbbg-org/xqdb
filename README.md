# XQDB

A high-performance connector to kdb+/q with Python (Narwhals/Arrow) and Node.js (TypeScript) bindings.

XQDB is independent and not affiliated with or endorsed by KX. kdb+ is a trademark of KX.

## Overview

**XQDB** provides high-performance connectivity between Python and Node.js applications and kdb+/q processes. The core is written in Rust, with Python bindings built on PyO3, Narwhals, and the Arrow C Stream interface, plus Node.js bindings built on napi-rs.

### Features

- Synchronous and asynchronous queries to kdb+/q
- Full kdb+ IPC protocol v6 support
- Backend-independent eager DataFrame and Series exchange through the Arrow C Stream interface
- TLS encryption and authentication
- Automatic retry with exponential backoff
- Subscription support for real-time data
- Read q binary table files directly into DataFrames
- Serialize data as kdb+ IPC bytes without a connection

## Project Structure

| Directory            | Description                              |
| -------------------- | ---------------------------------------- |
| `crates/xqdb`        | Core Rust library (connector, IPC serde) |
| `py-xqdb`            | Python bindings (PyO3 + Narwhals)         |
| `js-xqdb`            | Node.js and TypeScript package            |
| `bindings/napi-xqdb` | Shared napi-rs native binding layer      |

## Installation

### Python

**Requirements**: Python ≥ 3.10, Narwhals ≥ 2.10, PyArrow ≥ 20.0.0; pandas and Polars are optional backend packages

Install the published package:

```bash
python -m pip install xqdb
```

To build the Python package from source with setuptools-rust:

```bash
python -m pip install -e .
```

### Node.js

**Requirements**: Node.js ≥ 20

Install the published package:

```bash
npm install @xbbg/xqdb
```

To build the Node.js package from source for development:

```bash
cd js-xqdb
npm install
npm run build
```

## Quick Start

### Python

```python
import narwhals as nw
import xqdb

# Query with PyArrow backend (default)
conn = xqdb.Q("localhost", 1800, backend="pyarrow")

# Query
result = conn.sync("select from trade where date=last date")

# Extract native DataFrame: PyArrow, pandas, or Polars
df = nw.to_native(result)

# Send data (Narwhals or native eager DataFrame)
conn.sync("upsert", "table", df)

conn.disconnect()
```

### Node.js

```ts
import { Q } from "@xbbg/xqdb";

const conn = await Q.connect({
  host: "localhost",
  port: 1800,
});

try {
  const result = await conn.sync("select from trade where date=last date");
  await conn.asyn("upsert", "table", ["AAPL", 10n]);
  console.log(result);
} finally {
  await conn.disconnect();
}
```

## Benchmarks

Every q IPC client that can be legally and technically measured, against one
fixed KDB-X 5.0 fixture: 100,000 rows, seed 42, 50 measured rounds per subject
per operation, subject order reshuffled every round. `trade` is 14 columns,
`wide` is 64 columns, `depth` has two nested 5-float list columns. Throughput
divides the server's `count -8!table` by the median duration.

These are client-side measurements against a fixed server, not a claim about
kdb+ or KDB-X performance. Full methodology, fidelity matrix, and raw reports:
[`benchmarks/README.md`](benchmarks/README.md).

### Node.js — Node 26.3.0, win32-x64

★ marks the fastest measured client for that operation.

| Operation      | XQDB                    | jkdb 1.4.0        | node-q 2.7.0      |
| -------------- | ----------------------- | ----------------- | ----------------- |
| `read trade`   | ★ **24.8 ms** 424 MiB/s | 186.9 ms (7.6x)   | 198.1 ms (8.0x)   |
| `read wide`    | ★ **91.0 ms** 535 MiB/s | 3072.7 ms (33.8x) | 3197.6 ms (35.2x) |
| `read depth`   | ★ **26.3 ms** 409 MiB/s | 203.0 ms (7.7x)   | 201.6 ms (7.7x)   |
| `send trade`   | ★ **26.7 ms** 393 MiB/s | 46.4 ms (1.7x)    | not comparable    |
| `send wide`    | ★ **137.1 ms** 355 MiB/s | 179.4 ms (1.3x)  | not comparable    |
| `send depth`   | ★ **28.8 ms** 374 MiB/s | 54.9 ms (1.9x)    | not comparable    |
| scalar round trip | 0.351 ms             | ★ 0.346 ms        | 0.361 ms          |

XQDB is fastest on every table operation. jkdb takes the scalar round trip by
5 microseconds, which is the latency floor of a single request rather than a
codec difference.

### Python — CPython 3.12.13, win-amd64

Ratios are against XQDB's PyArrow backend. kola returns Polars and qconnect
returns pandas, so the report also carries a ratio against the XQDB backend
that materialises the same frame type. XQDB's PyArrow backend is fastest on
every operation.

| Operation      | XQDB pyarrow            | XQDB polars | XQDB pandas | kola 2.5.1        | qconnect 0.1.6    |
| -------------- | ----------------------- | ----------- | ----------- | ----------------- | ----------------- |
| `read trade`   | ★ **14.6 ms** 717 MiB/s | 15.3 ms     | 17.6 ms     | 17.7 ms (1.2x)    | 97.7 ms (6.7x)    |
| `read wide`    | ★ **50.3 ms** 967 MiB/s | 51.2 ms     | 54.7 ms     | 105.8 ms (2.1x)   | 191.2 ms (3.8x)   |
| `read depth`   | ★ **18.3 ms** 590 MiB/s | 19.1 ms     | 35.7 ms     | 20.9 ms (1.1x)    | 11710.9 ms (642x) |
| `send trade`   | ★ **20.3 ms** 517 MiB/s | 20.6 ms     | 23.3 ms     | 24.5 ms (1.2x)    | 64.0 ms (3.2x)    |
| `send wide`    | ★ **111.8 ms** 435 MiB/s | 115.7 ms   | 131.5 ms    | 124.0 ms (1.1x)   | 262.9 ms (2.4x)   |
| `send depth`   | ★ **23.6 ms** 457 MiB/s | 23.8 ms     | 54.1 ms     | aborts, see below | 2186.0 ms (92.7x) |
| scalar round trip | ★ 0.347 ms           | 0.366 ms    | 0.352 ms    | 0.350 ms          | 0.400 ms          |

### Correctness, measured alongside speed

Speed is only ranked where subjects do the same work. Each subject's decoded
value is sent back to q and compared with `~` before anything is timed.

- XQDB and jkdb round-trip all three tables to a q-identical value. XQDB raises
  rather than truncate a sub-microsecond timestamp atom into a Python
  `datetime`; kola rounds it to microseconds silently.
- `node-q` decodes int64 to double (`9007199254740993` reads back as `…992`) and
  timestamps to millisecond `Date`, and no decoded table re-encodes to a
  q-identical value, so it is excluded from every `send` rather than credited
  with encoding a different value.
- `kola@2.5.1` panics in its Rust serializer and aborts the process when sending
  a frame with list columns (`crates/kola/src/serde6.rs:1852`), so it is
  excluded from `send depth`; its `depth` read is unaffected.
- Not measured: `pykx` (its licence forbids publishing performance
  comparisons), `qpython`/`qpython3` (require numpy<1.20 and Python<=3.9;
  `qconnect` is the maintained fork measured in their place), and `pyq` (embeds
  Python inside q rather than acting as a client).

## Documentation

- [Python API Reference](py-xqdb/README.md) — Comprehensive API documentation and type mapping for Python/Narwhals bindings
- [Node.js API Reference](js-xqdb/README.md) — Comprehensive API documentation and value mapping for Node.js/TypeScript bindings

## License

XQDB is licensed under the [BSD-3-Clause](LICENSE) permissive open-source license, which permits use in proprietary and commercial applications.
