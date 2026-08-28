# XQDB client benchmarks

Two harnesses compare XQDB against every q IPC client we can legally and
technically measure, on one fixed fixture server:

| Suite    | Entry point                 | Subjects                                                     |
| -------- | --------------------------- | ------------------------------------------------------------ |
| Node.js  | `benchmarks/node/bench.mjs` | `@xbbg/xqdb`, `jkdb@1.4.0`, `node-q@2.7.0`                   |
| Python   | `benchmarks/python/bench.py`| `xqdb` (pyarrow/polars/pandas), `kola@2.5.1`, `qconnect@0.1.6` |

Every number is a client-side measurement against a fixed server. Nothing here
is a claim about kdb+ or KDB-X performance.

## Running

Both suites need the fixture container from `testing/kdb`, which requires
`KX_BEARER_TOKEN` and `KDB_LICENSE_B64` (see `testing/kdb/.env`).

```bash
task benchmark-node-podman     # builds js-xqdb, starts the fixture, runs, stops
task benchmark-python-podman
```

Against an already-running fixture:

```bash
cd benchmarks/node && npm install && node --expose-gc bench.mjs
python benchmarks/python/bench.py
```

Flags (both suites): `--host`, `--port`, `--warmups`, `--iterations`,
`--memory-results`, `--seed`, `--output`, `--samples`. Environment equivalents:
`XQDB_TEST_Q_HOST`, `XQDB_TEST_Q_PORT`, `XQDB_BENCH_WARMUPS`,
`XQDB_BENCH_ITERATIONS`, `XQDB_BENCH_MEMORY_RESULTS`, `XQDB_BENCH_SEED`.

Each run prints a summary table and writes a JSON report to
`benchmarks/results/`. `--samples` adds every raw duration and the per-round
subject order.

## Fixture

`testing/kdb` builds KDB-X 5.0.20260723 into a container and loads
`testing/kdb/init.q`, which seeds `\S 42` and publishes `.xqdb.rows`,
`.xqdb.seed`, and three tables:

| Table   | Columns | Shape stress                                              |
| ------- | ------- | --------------------------------------------------------- |
| `trade` | 14      | narrow: symbol, timestamp, long, char vector, 10 floats   |
| `wide`  | 64      | column count                                              |
| `depth` | 5       | nested: `ask` and `bid` are a 5-float list per row         |

Row count comes from `XQDB_Q_ROWS` (benchmarks default to `100000`). Before
timing, every subject is asked for `.xqdb.rows`, `.z.K`, and `.xqdb.seed`, and
the run aborts unless all subjects agree.

## Operations

| Operation       | Work                                                          |
| --------------- | ------------------------------------------------------------- |
| `scalar`        | `6f*7f` round trip: the latency floor                         |
| `read.<table>`  | query the table and materialise the client's own frame type    |
| `send.<table>`  | send the client's own decoded frame to `{[x]count x}`          |

`payloadBytes` is the server's `count -8!<table>`: one logical payload size
shared by all subjects, not observed wire bytes. Throughput is that size divided
by the median duration.

## Method

- **Order.** Each round runs every subject once, in an order reshuffled from
  `--seed`. A cyclic rotation is not sufficient: rotating by one preserves the
  adjacency relation, so every subject keeps the same predecessor in every round.
  That was measurable — `xqdb-pyarrow` sat behind `qconnect`'s one-second
  `depth` decode in every sample and reported 26.7 ms for a 2.8 ms operation.
- **Timing.** `process.hrtime.bigint` / `time.perf_counter_ns` around one
  request, one in flight per subject. Percentiles are nearest-rank on raw
  durations.
- **Validation.** Every measured result is checked after its timed interval.
- **Memory.** RSS/heap delta around a set of retained decoded frames with a
  forced GC at both snapshots. A noisy process-wide diagnostic, not a library
  footprint.
- **Python preflight isolation.** The Python preflight runs one subprocess per
  subject, emitting a JSON line per completed check, because a subject can abort
  the interpreter instead of raising (see kola below). The line-per-check
  protocol pins a fatal failure to the exact operation that caused it.

### Comparability gates

Speed is only reported where the subjects are doing the same work:

- `scalar` is value-exact. A subject that cannot return the exact value is
  listed in `unsupported`, not ranked.
- `read.<table>` is ranked for every subject that can decode the table, because
  the server sends all of them identical bytes and each library's own frame type
  is its product. Subjects whose value could not be proven to survive a q round
  trip appear in `roundTripUnverifiedSubjects` — a statement about the round
  trip, since the encoder can be the failing half.
- `send.<table>` is ranked only where the subject's decoded frame re-encodes to
  a value that q reports as `~`-identical to the fixture **and** of the same
  `count -8!x` size. Otherwise the subject would be timed encoding a different
  value, so it is listed in `unsupported` with no samples, throughput, or ratio.
- int64 and nanosecond-timestamp exactness are preflight facts, never timed.
  Timing a wrong decode against a right one compares different work.

Python reports two ratios because the subjects return different frame types:
`medianRatioVsReference` against `xqdb-pyarrow`, and
`medianRatioVsSameFrameXqdb` against the XQDB backend that materialises the same
frame type as the subject (Polars for kola, pandas for qconnect).

## Fidelity findings

These come out of the preflight, and they are the reason the gates exist.

- **`node-q@2.7.0`** decodes int64 to double (`9007199254740993` reads back as
  `…992`) and timestamps to millisecond `Date`. No decoded table re-encodes to a
  q-identical value, so it is excluded from every `send`. It is measured with
  `flipTables:false`, its fastest documented mode and the one whose shape is
  closest to the other subjects; `long2number:false` plus `nanos2date:false`
  restore exactness but roughly double decode time.
- **`jkdb@1.4.0`** needs `includeNanosecond:true` for sub-millisecond
  timestamps, which turns temporal values into text that its own encoder then
  rejects. The suite therefore keeps a second connection for the temporal
  fidelity check and measures the timed operations on the default connection.
- **`kola@2.5.1`** panics in its Rust serializer and aborts the process when
  asked to send a frame with list columns:
  `range end index 80040 out of range for slice of length 80000` at
  `crates/kola/src/serde6.rs:1852`. It is excluded from `send.depth`; its
  `depth` read is unaffected. kola also rounds nanosecond timestamps to
  microseconds silently.
- **`xqdb`** raises `ValueError` rather than truncate a sub-microsecond
  timestamp atom into a Python `datetime`, so its `nanosecondTimestamp` is
  `rejected` rather than `lossy`.

## Subjects we do not measure

| Candidate                | Why not                                                                                                                       |
| ------------------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| `pykx`                   | The licence covering its bundled q runtime forbids making performance comparisons available to third parties.                  |
| `qpython`, `qpython3`    | Both dereference numpy aliases removed in numpy 2 (`np.string_`, `np.bool`), so they need numpy<1.20 and Python<=3.9. `qconnect` is the maintained fork, measured in their place. |
| `pyq`                    | Embeds Python inside q rather than acting as a client.                                                                        |
| npm `kx`, `qnode`        | Unrelated packages that happen to own the names.                                                                              |
