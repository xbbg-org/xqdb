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
npm install xqdb
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
import { Q } from "xqdb";

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

## Documentation

- [Python API Reference](py-xqdb/README.md) — Comprehensive API documentation and type mapping for Python/Narwhals bindings
- [Node.js API Reference](js-xqdb/README.md) — Comprehensive API documentation and value mapping for Node.js/TypeScript bindings

## License

XQDB is licensed under the [BSD-3-Clause](LICENSE) permissive open-source license, which permits use in proprietary and commercial applications.
