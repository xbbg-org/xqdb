# XQDB — Node.js Bindings

XQDB is independent and not affiliated with or endorsed by KX. kdb+ is a trademark of KX.

`@xbbg/xqdb` is the ESM Node.js binding for XQDB's q IPC client. It exposes a strict TypeScript facade, keeps q tables and typed vectors columnar with Apache Arrow, and runs connection work on a dedicated native worker instead of the JavaScript event loop.

## Installation

**Requirements**: Node.js ≥ 20

Install the published package:

```bash
npm install @xbbg/xqdb
```

The install selects the matching optional native package for Windows x64, Linux x64 with glibc 2.28 or newer, or macOS arm64.

To build from source for development, install a Rust toolchain and run:

```bash
# From the js-xqdb directory
npm install
npm run build
```

`npm run build:native` calls the napi-rs v3 CLI with `bindings/napi-xqdb/Cargo.toml` and generates the internal `native.js` loader, `native.d.ts`, and local `.node` artifact in this package. The generated loader checks the local artifact during development. The public entry point does not export the generated native declarations.

## Connect and query

```ts
import { Q } from "@xbbg/xqdb";

const q = await Q.connect({
  host: "localhost",
  port: 1800,
  user: "user",
  password: "password",
  tls: true,
  timeout: 30_000,
  retries: 2,
});

try {
  const result = await q.sync("select from trade");
  await q.asyn("insert", "trade", ["AAPL", 10n]);
  console.log(result);
} finally {
  await q.disconnect();
}
```

q IPC authentication sends credentials in cleartext when TLS is disabled. Enable `tls` for credentialed connections unless another trusted transport already protects the socket.

`connect()`, `disconnect()`, `sync()`, `asyn()`, and `receive()` all return promises. `sync()` is a synchronous q IPC request/response transaction, not a synchronous JavaScript function. Calls made on one `Q` are admitted to a bounded, eight-command native FIFO in call order so complete IPC transactions cannot interleave. Admission never waits on the JavaScript thread: when all eight pending slots are occupied, the new call fails with `XQDB_BACKPRESSURE`.

The default socket timeout is 30,000 milliseconds. `timeout` must be positive and cannot exceed 24 hours (86,400,000 milliseconds); the native layer rounds it up to the next whole second. The finite timeout lets a blocked receive eventually release the connection and its worker. `retries` is the number of additional attempts made by an explicit `connect()` after an IO failure; authentication failures are not retried. Query methods can establish the native connection automatically when needed.

q stores symbols and strings as raw bytes and never validates them, while Arrow string columns must be valid UTF-8. With the default `symbolEncoding: "strict"`, a result carrying a stray Latin-1 or binary byte in a symbol, symbol column, string column, char column, or lambda fails with `XQDB_CONVERSION` naming the offending value. Opt in with `symbolEncoding: "lossy"` to decode such text with each maximal invalid sequence replaced by `"\uFFFD"`, so every other value in the result survives intact. Valid text decodes identically under both policies, and q error messages always surface with replacement characters rather than being hidden behind a decoding failure.

Each submitted argument set is limited to 64 MiB after native snapshot accounting. Message size itself carries no fixed ceiling: a q result is bounded only by available memory and by the q header's 40-bit length field. Every process-sized buffer the native layer allocates — message body, decompression target, column data, and Arrow validity bitmaps — is reserved fallibly, so a result too large for the machine returns an error instead of terminating the process. A declared length on its own reserves at most 32 MiB — beyond that the response buffer grows eightfold only once the peer has filled the previous reservation, so the memory committed stays within eight times the bytes actually delivered — and a compressed response whose declared decompressed size is unreachable from the bytes received is rejected before its buffer is allocated, because the q IPC decompressor expands its input by at most 121x.

`disconnect()` is idempotent. A `Q` can reconnect after disconnect:

```ts
await q.disconnect();
await q.connect();
const value = await q.sync("42");
```

Always call `disconnect()` in `finally` or an equivalent cleanup hook instead of relying on garbage collection.

### Operators and lambdas

Pass q primitives and arbitrary lambdas as first-class arguments:

```ts
import { XqdbQLambda, XqdbQOperator } from "@xbbg/xqdb";

await q.sync("{[op;a;b] .[op;(a;b)]}", XqdbQOperator.PLUS, 1, 2);
await q.sync(
  "{[op;a;b] .[op;(a;b)]}",
  new XqdbQLambda("{x+y}"),
  1,
  2,
);

const scoped = new XqdbQLambda("{x+y}", "analytics");
```

`new XqdbQOperator(name)` accepts supported q primitive names such as `"+"`; it does not expose wire opcodes. `new XqdbQLambda(source, context = "")` preserves its source text, requires a brace-delimited UTF-8 body (optionally prefixed with `k)`), rejects NUL bytes in both fields, and rejects context values beginning with `"."`. The context `"analytics"` represents q namespace `.analytics` because the wire context omits the leading dot. Lambda source is executable q code: construct it only from trusted input.

## Value mapping

### JavaScript to q

| JavaScript input | q value |
| --- | --- |
| `null` | generic null |
| `boolean` | boolean |
| `number` | float |
| `bigint` | long |
| `string` | symbol; embedded NUL is rejected |
| `Buffer` or `Uint8Array` | char vector, preserving arbitrary bytes |
| ordinary array | mixed list |
| plain string-keyed object | dictionary; keys cannot contain NUL; `{}` becomes `` (`symbol$())!() `` |
| Apache Arrow `Vector` | typed series/list through Arrow IPC |
| Apache Arrow `Table` | table through Arrow IPC |
| `XqdbTimestamp` | timestamp as Unix-epoch nanoseconds |
| `XqdbDate` | date in `YYYY-MM-DD` form |
| `XqdbTime` | millisecond-aligned nanoseconds since midnight |
| `XqdbTimespan` | signed nanosecond duration |
| `XqdbQOperator` | supported primitive operator |
| `XqdbQLambda` | lambda source and q context |

### q to JavaScript

| q value | JavaScript output |
| --- | --- |
| boolean and safe-width numeric atoms | `boolean` or `number` |
| long | `bigint` |
| symbol, string, or GUID | `string` |
| char atom | byte value as `number` |
| char vector | `Buffer` |
| timestamp | `XqdbTimestamp` with a `bigint` nanosecond payload |
| date | `XqdbDate` |
| time | `XqdbTime` with a `bigint` nanosecond payload |
| timespan | `XqdbTimespan` with a `bigint` nanosecond payload |
| primitive operator | `XqdbQOperator` |
| lambda | `XqdbQLambda` |
| typed list | Apache Arrow `Vector` |
| mixed list | array |
| dictionary | plain string-keyed object; an empty dictionary such as `()!()` becomes `{}` |
| table | Apache Arrow `Table` |

The temporal wrappers prevent nanosecond values from being rounded through JavaScript `number` or `Date`:

```ts
import { XqdbTime, XqdbTimespan, XqdbTimestamp } from "@xbbg/xqdb";

const timestamp = new XqdbTimestamp(1_725_000_000_000_000_001n);
const noon = new XqdbTime(43_200_000_000_000n);
const oneNanosecondAgo = new XqdbTimespan(-1n);
```

Tables and typed lists cross the native boundary as Arrow IPC streams and are materialized as Arrow `Table` and `Vector` objects. They are not expanded into row objects. The transfer is columnar, but this package does not claim zero-copy transfer across the N-API boundary.

Top-level `Buffer` and `Uint8Array` values remain lossless for arbitrary bytes. q char data inside Arrow table or nested columns must be valid UTF-8; invalid bytes return a conversion error instead of being replaced or panicking. Because a q char atom is one byte while each Arrow string cell must be valid UTF-8, direct char-atom columns are limited to ASCII. Use a top-level byte value when arbitrary-byte round trips are required.

## Binary helpers

Both helpers are asynchronous because their native parsing and serialization work runs away from the JavaScript event loop:

```ts
import { readBinary6, serializeAsIpcBytes6 } from "@xbbg/xqdb";

const table = await readBinary6("trade.bin");
const legacy = await readBinary6("legacy.bin", { symbolEncoding: "lossy" });
const message = await serializeAsIpcBytes6("sync", true, table);
```

`readBinary6()` resolves to an Arrow `Table` and accepts the same `symbolEncoding` policy as `Q`. `serializeAsIpcBytes6()` resolves to a Node.js `Buffer` containing one q IPC message.

`readBinary6()` accepts regular local files that fit in available memory and rejects Windows UNC/device paths. It imposes no fixed size limit: a Kxzip-compressed file is rejected only when its declared decompressed size exceeds what its own LZ4 blocks can hold, which is the smaller of each block's uncompressed size and 255x the compressed bytes actually present.

## Subscriptions

A pending `receive()` occupies its q connection until a message arrives or the socket timeout expires. A receive timeout fails with `XQDB_IO`, and the core closes that socket; callers must reconnect and resubscribe before receiving again. Use a dedicated `Q` for subscriptions and another for ordinary queries:

```ts
import { XqdbIOError, Q } from "@xbbg/xqdb";

const queries = await Q.connect(options);
const subscription = await Q.connect(options);

try {
  await subscription.asyn(".u.sub", "trade", "");
  for (;;) {
    try {
      const update = await subscription.receive();
      consume(update);
    } catch (error) {
      if (!(error instanceof XqdbIOError) || error.code !== "XQDB_IO") {
        throw error;
      }
      await subscription.connect();
      await subscription.asyn(".u.sub", "trade", "");
    }
  }
} finally {
  await Promise.allSettled([subscription.disconnect(), queries.disconnect()]);
}
```

Alternatively, set `timeout` above the maximum expected quiet interval, up to the 24-hour limit. The first release intentionally has no connection pool or separate subscription engine.

## Errors

Native failures become stable public errors:

- `XqdbIOError` for `XQDB_IO` transport and connection failures
- `XqdbAuthError` for `XQDB_AUTH` authentication failures
- `XqdbError` with code `XQDB_BACKPRESSURE` when a connection's native FIFO is full
- `XqdbError` for server, conversion, unsupported-value, internal, and other generic native failures

Every error has a stable `code`, its original native text in `nativeMessage`, and the native payload or rejected exception in `cause`. Failure to resolve a local or installed addon is a `XqdbIOError` with code `XQDB_NATIVE_LOAD` and remediation in its message.

## Supported native targets

| Platform | Native package | Requirement |
| --- | --- | --- |
| Windows x64 | `@xbbg/xqdb-win32-x64-msvc` | Microsoft x64 ABI |
| Linux x64 | `@xbbg/xqdb-linux-x64-gnu` | glibc 2.28 or newer |
| macOS arm64 | `@xbbg/xqdb-darwin-arm64` | Apple Silicon |

Other operating-system, CPU, and libc combinations are unsupported in the initial release. A generated napi-rs loader reports an unsupported target or a missing optional binary instead of silently falling back to a different build.

## TLS certificate behavior

`tls: true` encrypts the connection and verifies the server certificate and hostname against the operating system's trusted certificate store. Connections with an invalid, expired, untrusted, or hostname-mismatched certificate fail.

## License

XQDB is licensed under the [BSD-3-Clause](../LICENSE) permissive open-source license, which permits use in proprietary and commercial applications.
