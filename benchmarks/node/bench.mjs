// Node.js q IPC client comparison against a single fixed KDB-X fixture.
//
// Subjects are measured round-robin with a rotating start position so machine
// and server drift is shared instead of accumulating on one subject. Every
// subject is configured in its fastest documented mode.
//
// Fidelity is a preflight, not an afterthought:
//
//   * `scalar` is a value-exact latency floor, so a subject that cannot return
//     the exact value is excluded from it.
//   * `read.<table>` decodes an identical server byte stream into each
//     library's documented representation, so every subject is ranked. A
//     subject whose value could not be proven to survive a q round trip is
//     named in `roundTripUnverifiedSubjects`; that is a statement about the
//     round trip, not a proven decode loss, because the encoder can be the
//     failing half.
//   * `send.<table>` is only comparable when the subject's own decoded value
//     re-encodes to a q-identical value of the same canonical size; otherwise
//     the subject would be timed on different work, so it is listed in
//     `unsupported` with no samples, throughput, or ratio.
//
// int64 and nanosecond-timestamp exactness are reported by the preflight rather
// than timed: they are correctness claims, and timing a wrong decode against a
// right one would compare different work.

import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { mkdir, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { cpus } from "node:os";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { promisify } from "node:util";

import { Q, XqdbTimestamp } from "../../js-xqdb/dist/index.js";

const HERE = dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);
// Resolve from js-xqdb so reported versions are the ones its dist actually loads.
const packageRequire = createRequire(resolve(HERE, "../../js-xqdb/package.json"));

// `require("pkg/package.json")` fails for packages with a restrictive exports
// map (apache-arrow), so resolve the entry point and walk up to its manifest.
function packageVersion(specifier, resolver) {
  let directory = dirname(resolver.resolve(specifier));
  for (let depth = 0; depth < 8; depth += 1) {
    const candidate = resolve(directory, "package.json");
    if (existsSync(candidate)) {
      const manifest = JSON.parse(readFileSync(candidate, "utf8"));
      if (manifest.name === specifier) return manifest.version;
    }
    const parent = dirname(directory);
    if (parent === directory) break;
    directory = parent;
  }
  throw new Error(`could not determine installed version of ${specifier}`);
}

const TABLES = {
  trade: 14,
  wide: 64,
  depth: 5,
};
const SCALAR_EXPRESSION = "6f*7f";
const LONG_EXPRESSION = "9007199254740993j";
const LONG_EXPECTED = 9007199254740993n;
const TIMESTAMP_EXPRESSION = "2024.01.02D03:04:05.123456789";
const TIMESTAMP_EXPECTED_NS = 1704164645123456789n;
const COUNT_LAMBDA = "{[x]count x}";
const CANONICAL_BYTES_LAMBDA = "{[x]count -8!x}";

// ── options ──────────────────────────────────────────────────────────────────

function parseOptions(argv) {
  const options = {
    host: process.env.XQDB_TEST_Q_HOST ?? "127.0.0.1",
    port: Number(process.env.XQDB_TEST_Q_PORT ?? 1801),
    warmups: Number(process.env.XQDB_BENCH_WARMUPS ?? 3),
    iterations: Number(process.env.XQDB_BENCH_ITERATIONS ?? 50),
    memoryResults: Number(process.env.XQDB_BENCH_MEMORY_RESULTS ?? 5),
    seed: Number(process.env.XQDB_BENCH_SEED ?? 42),
    output: undefined,
    samples: false,
  };
  const numeric = {
    "--port": "port",
    "--warmups": "warmups",
    "--iterations": "iterations",
    "--memory-results": "memoryResults",
    "--seed": "seed",
  };
  for (let index = 0; index < argv.length; index += 1) {
    const flag = argv[index];
    if (flag === "--samples") {
      options.samples = true;
      continue;
    }
    const value = argv[index + 1];
    if (value === undefined) throw new Error(`${flag} requires a value`);
    index += 1;
    if (flag === "--host") options.host = value;
    else if (flag === "--output") options.output = value;
    else if (numeric[flag] !== undefined) options[numeric[flag]] = Number(value);
    else throw new Error(`unknown flag: ${flag}`);
  }
  if (!Number.isInteger(options.port) || options.port < 1) throw new Error("--port must be a positive integer");
  if (!Number.isInteger(options.warmups) || options.warmups < 0) throw new Error("--warmups must be non-negative");
  if (!Number.isInteger(options.iterations) || options.iterations < 1) throw new Error("--iterations must be positive");
  if (!Number.isInteger(options.memoryResults) || options.memoryResults < 1) {
    throw new Error("--memory-results must be positive");
  }
  if (!Number.isInteger(options.seed)) throw new Error("--seed must be an integer");
  return options;
}

// ── subjects ─────────────────────────────────────────────────────────────────

async function createXqdb({ host, port }) {
  const manifest = packageRequire("./package.json");
  const connection = await Q.connect({ host, port });
  return {
    id: "xqdb",
    package: manifest.name,
    version: manifest.version,
    implementation: "Rust core via napi-rs, Arrow C Stream decode",
    representation: "apache-arrow Table",
    config: {},
    notes: [],
    eval: (expression) => connection.sync(expression),
    apply: (lambda, value) => connection.sync(lambda, value),
    read: (table) => connection.sync(table),
    shapeOf: (value) =>
      typeof value?.numRows === "number" && Array.isArray(value?.schema?.fields)
        ? { rows: value.numRows, columns: value.schema.fields.length }
        : null,
    longOf: (value) => (typeof value === "bigint" ? value : null),
    timestampNanosOf: (value) => (value instanceof XqdbTimestamp ? value.nanoseconds : null),
    close: () => connection.disconnect(),
  };
}

async function createJkdb({ host, port }) {
  const manifest = require("jkdb/package.json");
  const { QConnection } = require("jkdb");
  const primary = new QConnection({ host, port, useBigInt: true });
  await primary.connectAsync();
  // jkdb decodes sub-millisecond timestamps only with includeNanosecond, which
  // switches temporal values to text and makes its encoder reject them. Keep it
  // on a second connection so the temporal fidelity claim stays measurable
  // without changing the representation used for the timed operations.
  const nanosecond = new QConnection({ host, port, useBigInt: true, includeNanosecond: true });
  await nanosecond.connectAsync();
  return {
    id: "jkdb",
    package: manifest.name,
    version: manifest.version,
    implementation: "Rust core via napi-rs, plain JS objects",
    representation: 'column-oriented object with Symbol.for("meta") schema',
    config: { useBigInt: true, includeNanosecond: false },
    notes: [
      "sub-millisecond timestamps need includeNanosecond:true, measured here on a second connection; in that mode temporal values become text and the encoder rejects them, so tables cannot be sent back",
    ],
    eval: (expression) => primary.syncAsync(expression),
    apply: (lambda, value) => primary.syncAsync([lambda, value]),
    read: (table) => primary.syncAsync(table),
    shapeOf: (value) => {
      const meta = value?.[Symbol.for("meta")];
      if (!meta || !Array.isArray(meta.c)) return null;
      return { rows: value[meta.c[0]]?.length ?? 0, columns: meta.c.length };
    },
    longOf: (value) => (typeof value === "bigint" ? value : null),
    timestampNanosOf: async () => {
      const text = await nanosecond.syncAsync(TIMESTAMP_EXPRESSION);
      if (typeof text !== "string") return null;
      const [seconds, fraction = ""] = text.split(".");
      const epochMs = BigInt(Date.parse(`${seconds}Z`));
      return epochMs * 1_000_000n + BigInt(fraction.padEnd(9, "0").slice(0, 9));
    },
    close: async () => {
      await primary.closeAsync();
      await nanosecond.closeAsync();
    },
  };
}

async function createNodeQ({ host, port }) {
  const manifest = require("node-q/package.json");
  const nodeq = require("node-q");
  // flipTables:false keeps node-q column-oriented, which is both its fastest
  // decode path and the shape closest to the other subjects. long2number and
  // nanos2date keep their defaults because the lossless alternatives allocate a
  // wrapper object per element and roughly double decode time.
  const connect = promisify(nodeq.connect.bind(nodeq));
  const connection = await connect({ host, port, flipTables: false });
  const k = promisify(connection.k.bind(connection));
  return {
    id: "node-q",
    package: manifest.name,
    version: manifest.version,
    implementation: "pure JavaScript codec",
    representation: "column-oriented object (flipTables:false)",
    config: { flipTables: false },
    notes: [
      "int64 decodes to double and loses precision beyond 2^53; timestamps decode to millisecond Date",
      "long2number:false and nanos2date:false restore exactness with Long wrappers but roughly double decode time",
    ],
    eval: (expression) => k(expression),
    apply: (lambda, value) => k(lambda, value),
    read: (table) => k(table),
    shapeOf: (value) => {
      if (value === null || typeof value !== "object" || Array.isArray(value)) return null;
      const columns = Object.keys(value);
      return { rows: value[columns[0]]?.length ?? 0, columns: columns.length };
    },
    longOf: (value) => (typeof value === "number" && Number.isFinite(value) ? BigInt(value) : null),
    timestampNanosOf: (value) => (value instanceof Date ? BigInt(value.getTime()) * 1_000_000n : null),
    close: () => new Promise((done) => connection.close(done)),
  };
}

const SUBJECT_FACTORIES = [createXqdb, createJkdb, createNodeQ];

// ── statistics ───────────────────────────────────────────────────────────────

function nearestRank(ascending, fraction) {
  const rank = Math.round(fraction * (ascending.length - 1));
  return ascending[Math.max(0, Math.min(ascending.length - 1, rank))];
}

function metrics(samplesNs, payloadBytes, keepSamples) {
  const ascending = [...samplesNs].sort((left, right) => (left < right ? -1 : left > right ? 1 : 0));
  const toMs = (value) => Number(value) / 1e6;
  const total = samplesNs.reduce((sum, value) => sum + value, 0n);
  const median =
    ascending.length % 2 === 1
      ? toMs(ascending[(ascending.length - 1) / 2])
      : (toMs(ascending[ascending.length / 2 - 1]) + toMs(ascending[ascending.length / 2])) / 2;
  const report = {
    iterations: samplesNs.length,
    minMs: toMs(ascending[0]),
    medianMs: median,
    meanMs: Number(total) / samplesNs.length / 1e6,
    p90Ms: toMs(nearestRank(ascending, 0.9)),
    p99Ms: toMs(nearestRank(ascending, 0.99)),
    maxMs: toMs(ascending[ascending.length - 1]),
  };
  if (payloadBytes !== null) {
    report.payloadBytes = payloadBytes;
    report.medianMibPerSecond = payloadBytes / (median / 1000) / 2 ** 20;
  }
  if (keepSamples) report.samplesMs = samplesNs.map(toMs);
  return report;
}

async function timed(operation) {
  const started = process.hrtime.bigint();
  const value = await operation();
  return { durationNs: process.hrtime.bigint() - started, value };
}

function rotate(list, offset) {
  // Only for untimed ordering: a cyclic shift keeps neighbours fixed.
  const shift = offset % list.length;
  return [...list.slice(shift), ...list.slice(0, shift)];
}

// Deterministic per-round order. A cyclic rotation is not good enough here:
// rotating by one preserves the adjacency relation, so every subject keeps the
// same predecessor in every round and a subject that follows an expensive
// neighbour pays for it in every sample. Reshuffling varies predecessors too.
function mulberry32(seed) {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let value = state;
    value = Math.imul(value ^ (value >>> 15), value | 1);
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
    return ((value ^ (value >>> 14)) >>> 0) / 4294967296;
  };
}

function shuffled(list, seed, roundIndex) {
  const next = mulberry32(Math.imul(seed, 1000003) + roundIndex);
  const order = [...list];
  for (let index = order.length - 1; index > 0; index -= 1) {
    const swap = Math.floor(next() * (index + 1));
    [order[index], order[swap]] = [order[swap], order[index]];
  }
  return order;
}

// ── progress ─────────────────────────────────────────────────────────────────

// Progress on stderr so stdout stays the summary. A full run is minutes long
// and a single subject can hold a round for seconds, so silence is
// indistinguishable from a hang. A terminal gets a rewritten line per round; a
// captured log gets one line every few seconds so it stays readable.
const PROGRESS_THROTTLE_MS = 3000;
const progress = {
  interactive: process.stderr.isTTY === true,
  pending: false,
  lastStep: 0,
  write(text, transient) {
    if (transient && this.interactive) {
      process.stderr.write(`\r${text.padEnd(96).slice(0, 96)}`);
      this.pending = true;
      return;
    }
    if (this.pending) {
      process.stderr.write("\n");
      this.pending = false;
    }
    process.stderr.write(`${text}\n`);
  },
  line(text) {
    this.lastStep = 0;
    this.write(text, false);
  },
  step(text, force = false) {
    const now = Date.now();
    if (!this.interactive && !force && now - this.lastStep < PROGRESS_THROTTLE_MS) return;
    this.lastStep = now;
    this.write(text, true);
  },
};

function formatDuration(seconds) {
  return seconds < 90
    ? `${seconds.toFixed(0)}s`
    : `${Math.floor(seconds / 60)}m${String(Math.floor(seconds) % 60).padStart(2, "0")}s`;
}

// ── measurement ──────────────────────────────────────────────────────────────

async function runOperation({ ids, operations, warmups, iterations, seed, check, label }) {
  for (let round = 0; round < warmups; round += 1) {
    progress.step(`${label} warmup ${round + 1}/${warmups}`);
    for (const id of shuffled(ids, seed, -1 - round)) await operations[id]();
  }
  const samples = Object.fromEntries(ids.map((id) => [id, []]));
  const order = [];
  const startedRun = process.hrtime.bigint();
  for (let round = 0; round < iterations; round += 1) {
    const sequence = shuffled(ids, seed, round);
    order.push(sequence);
    for (const id of sequence) {
      const measured = await timed(operations[id]);
      if (check) check(id, measured.value);
      samples[id].push(measured.durationNs);
    }
    const elapsed = Number(process.hrtime.bigint() - startedRun) / 1e9;
    progress.step(
      `${label} ${round + 1}/${iterations} rounds, ${formatDuration(elapsed)} elapsed, ${formatDuration(
        (elapsed / (round + 1)) * (iterations - round - 1),
      )} left`,
    );
  }
  return { samples, order };
}

function memorySnapshot() {
  const usage = process.memoryUsage();
  return { rss: usage.rss, heapUsed: usage.heapUsed, external: usage.external, arrayBuffers: usage.arrayBuffers };
}

async function retainedMemory(count, operation) {
  globalThis.gc();
  const before = memorySnapshot();
  const retained = [];
  for (let index = 0; index < count; index += 1) retained.push(await operation());
  globalThis.gc();
  const after = memorySnapshot();
  assert.equal(retained.length, count);
  const deltaBytes = Object.fromEntries(Object.keys(before).map((key) => [key, after[key] - before[key]]));
  retained.length = 0;
  globalThis.gc();
  return { retainedResults: count, deltaBytes };
}

// ── fidelity preflight ───────────────────────────────────────────────────────

async function measureFidelity(subject, fixture) {
  const scalar = await subject.eval(SCALAR_EXPRESSION);
  const decodedLong = await subject.longOf(await subject.eval(LONG_EXPRESSION));
  const decodedNanos = await subject.timestampNanosOf(await subject.eval(TIMESTAMP_EXPRESSION));
  const report = {
    scalarExact: Number(scalar) === 42,
    scalarDetail: String(scalar),
    int64Exact: decodedLong === LONG_EXPECTED,
    int64Decoded: decodedLong === null ? null : decodedLong.toString(),
    nanosecondTimestampExact: decodedNanos === TIMESTAMP_EXPECTED_NS,
    nanosecondTimestampDecoded: decodedNanos === null ? null : decodedNanos.toString(),
    tables: {},
  };
  for (const [table, columns] of Object.entries(TABLES)) {
    const value = await subject.read(table);
    const shape = await subject.shapeOf(value);
    const entry = {
      shape,
      shapeMatchesFixture: shape !== null && shape.rows === fixture.rows && shape.columns === columns,
      reencodesToIdenticalQValue: false,
      canonicalBytesMatchFixture: false,
    };
    try {
      entry.reencodesToIdenticalQValue = Boolean(await subject.apply(`{[x]${table}~x}`, value));
      entry.reencodedRows = Number(await subject.apply(COUNT_LAMBDA, value));
      entry.reencodedCanonicalBytes = Number(await subject.apply(CANONICAL_BYTES_LAMBDA, value));
      entry.canonicalBytesMatchFixture = entry.reencodedCanonicalBytes === fixture.tables[table].canonicalBytes;
    } catch (error) {
      entry.encodeError = error.message;
    }
    // Timing a send is only meaningful when the encoder produces the same q
    // value of the same size; anything else is different work.
    entry.sendComparable = entry.reencodesToIdenticalQValue && entry.canonicalBytesMatchFixture;
    if (!entry.sendComparable) {
      entry.sendExcludedBecause =
        entry.encodeError !== undefined
          ? `encoder rejected the decoded value: ${entry.encodeError}`
          : !entry.reencodesToIdenticalQValue
            ? "decoded value does not re-encode to a q-identical value"
            : `re-encoded canonical size ${entry.reencodedCanonicalBytes} != fixture ${fixture.tables[table].canonicalBytes}`;
    }
    // `differs` requires q to have actually compared the two values. When the
    // encoder threw, no comparison happened, so the round trip is unproven
    // rather than failed, and the summary must not claim a value difference.
    entry.roundTrip = entry.sendComparable
      ? "identical"
      : entry.encodeError !== undefined
        ? "unverified"
        : entry.reencodesToIdenticalQValue
          ? "resized"
          : "differs";
    report.tables[table] = entry;
  }
  return report;
}

// ── main ─────────────────────────────────────────────────────────────────────

async function main() {
  if (typeof globalThis.gc !== "function") {
    throw new Error("run with `node --expose-gc` so retained-result memory can be measured");
  }
  const options = parseOptions(process.argv.slice(2));
  const subjects = [];
  let report;
  try {
    for (const factory of SUBJECT_FACTORIES) subjects.push(await factory(options));
    const reference = subjects[0];
    assert.equal(reference.id, "xqdb", "xqdb must be the reference subject");
    const allIds = subjects.map((subject) => subject.id);
    const byId = Object.fromEntries(subjects.map((subject) => [subject.id, subject]));

    const fixture = {
      host: options.host,
      port: options.port,
      qVersion: Number(await reference.eval(".z.K")),
      rows: Number(await reference.eval(".xqdb.rows")),
      seed: Number(await reference.eval(".xqdb.seed")),
      tables: {},
    };
    for (const [table, columns] of Object.entries(TABLES)) {
      fixture.tables[table] = { columns, canonicalBytes: Number(await reference.eval(`count -8!${table}`)) };
    }
    // Every subject must agree on what it is talking to before anything is timed.
    for (const subject of subjects) {
      assert.equal(Number(await subject.eval(".xqdb.rows")), fixture.rows, `${subject.id}: fixture row mismatch`);
      assert.equal(Number(await subject.eval(".z.K")), fixture.qVersion, `${subject.id}: q version mismatch`);
      assert.equal(Number(await subject.eval(".xqdb.seed")), fixture.seed, `${subject.id}: fixture seed mismatch`);
    }

    const fidelity = {};
    for (const [index, subject] of subjects.entries()) {
      progress.step(`[preflight ${index + 1}/${subjects.length}] ${subject.id}`);
      fidelity[subject.id] = await measureFidelity(subject, fixture);
    }
    progress.line(`[preflight] ${subjects.length} subjects probed`);
    const totalOperations = 1 + 2 * Object.keys(TABLES).length;
    let operationIndex = 0;

    const operations = {};
    const order = {};

    async function record({ name, ids, payloadBytes, build, check, extra }) {
      operationIndex += 1;
      const label = `[bench ${operationIndex}/${totalOperations}] ${name} (${ids.length} subjects)`;
      progress.step(label);
      const startedLabel = process.hrtime.bigint();
      const run = await runOperation({
        ids,
        operations: Object.fromEntries(ids.map((id) => [id, build(byId[id])])),
        warmups: options.warmups,
        iterations: options.iterations,
        seed: options.seed,
        check,
        label,
      });
      const perSubject = Object.fromEntries(
        ids.map((id) => [id, metrics(run.samples[id], payloadBytes, options.samples)]),
      );
      const referenceMedian = perSubject[reference.id].medianMs;
      operations[name] = {
        payloadBytes,
        subjects: perSubject,
        medianRatioVsXqdb: Object.fromEntries(ids.map((id) => [id, perSubject[id].medianMs / referenceMedian])),
        ...extra,
      };
      if (options.samples) order[name] = run.order;
      progress.line(
        `${label} done in ${formatDuration(Number(process.hrtime.bigint() - startedLabel) / 1e9)}: ${ids
          .map((id) => `${id}=${perSubject[id].medianMs.toFixed(2)}ms`)
          .join("  ")}`,
      );
    }

    const scalarIds = allIds.filter((id) => fidelity[id].scalarExact);
    assert(scalarIds.includes(reference.id), "reference subject failed the scalar preflight");
    await record({
      name: "scalar",
      ids: scalarIds,
      payloadBytes: null,
      build: (subject) => () => subject.eval(SCALAR_EXPRESSION),
      extra: {
        unsupported: allIds
          .filter((id) => !scalarIds.includes(id))
          .map((id) => ({ subject: id, reason: `scalar decode is not exact: ${fidelity[id].scalarDetail}` })),
      },
    });

    for (const table of Object.keys(TABLES)) {
      const payloadBytes = fixture.tables[table].canonicalBytes;
      await record({
        name: `read.${table}`,
        ids: allIds,
        payloadBytes,
        build: (subject) => () => subject.read(table),
        check: (id, value) => {
          const shape = byId[id].shapeOf(value);
          assert.equal(shape?.rows, fixture.rows, `${id}: ${table} row count mismatch`);
        },
        extra: {
          roundTripUnverifiedSubjects: allIds.filter(
            (id) => !fidelity[id].tables[table].reencodesToIdenticalQValue,
          ),
          roundTripUnverifiedReasons: Object.fromEntries(
            allIds
              .filter((id) => !fidelity[id].tables[table].reencodesToIdenticalQValue)
              .map((id) => [id, fidelity[id].tables[table].sendExcludedBecause]),
          ),
        },
      });

      const sendableIds = allIds.filter((id) => fidelity[id].tables[table].sendComparable);
      assert(sendableIds.includes(reference.id), `${table}: reference subject cannot send comparably`);
      const decoded = {};
      for (const id of sendableIds) decoded[id] = await byId[id].read(table);
      await record({
        name: `send.${table}`,
        ids: sendableIds,
        payloadBytes,
        build: (subject) => () => subject.apply(COUNT_LAMBDA, decoded[subject.id]),
        check: (id, value) => assert.equal(Number(value), fixture.rows, `${id}: send count mismatch`),
        extra: {
          unsupported: allIds
            .filter((id) => !sendableIds.includes(id))
            .map((id) => ({ subject: id, reason: fidelity[id].tables[table].sendExcludedBecause })),
        },
      });
    }

    const memory = {};
    let tableIndex = 0;
    for (const table of Object.keys(TABLES)) {
      memory[table] = {};
      for (const id of rotate(allIds, tableIndex)) {
        progress.step(`[memory ${tableIndex + 1}/${Object.keys(TABLES).length}] ${table} ${id}`);
        memory[table][id] = await retainedMemory(options.memoryResults, () => byId[id].read(table));
      }
      tableIndex += 1;
    }
    progress.line(`[memory] ${Object.keys(TABLES).length} tables probed`);

    report = {
      schemaVersion: 1,
      suite: "node",
      generatedAt: new Date().toISOString(),
      fixture,
      runtime: {
        node: process.version,
        platform: process.platform,
        arch: process.arch,
        cpu: cpus()[0]?.model ?? "unavailable",
        arrow: packageVersion("apache-arrow", packageRequire),
      },
      method: {
        warmupsPerSubjectPerOperation: options.warmups,
        iterationsPerSubjectPerOperation: options.iterations,
        retainedResultsPerMemoryProbe: options.memoryResults,
        clock: "process.hrtime.bigint",
        orderSeed: options.seed,
        scheduling:
          "one request in flight per subject; every round runs each subject once in a deterministic order reshuffled from `orderSeed`, so positions and predecessors both vary; a cyclic rotation would pin every subject behind the same neighbour in every round",
        percentiles: "nearest-rank on raw durations",
        payloadBytes:
          "server-side `count -8!table` for the fixture table: a logical payload size shared by all subjects, not observed wire bytes",
        throughput: "payloadBytes divided by the median duration",
        memory:
          "process-wide RSS/heap delta around retained decoded results with forced GC at both snapshots; a noisy diagnostic, not a library footprint",
        referenceSubject: reference.id,
        scalarComparability:
          "value-exact latency floor; a subject that cannot return the exact value is listed in `unsupported` instead of being ranked",
        readComparability:
          "every subject decodes an identical server byte stream into its documented representation, so all are ranked; proven decode losses are the int64 and nanosecond checks in `fidelity`, while `roundTripUnverifiedSubjects` records only that the decode-then-encode round trip could not be proven, which the encoder alone can cause",
        sendComparability:
          "sends are compared only for subjects whose decoded value re-encodes to a q-identical value of the same canonical size; others are listed in `unsupported` with no samples, throughput, or ratio",
        untimedCorrectnessChecks:
          "int64 and nanosecond-timestamp exactness are reported by the preflight, not timed: timing a wrong decode against a right one would compare different work",
        excludedSubjects: {
          "@kxsystems/*": "KX publishes no Node.js q client on npm",
        },
      },
      subjects: subjects.map((subject) => ({
        id: subject.id,
        package: subject.package,
        version: subject.version,
        implementation: subject.implementation,
        representation: subject.representation,
        config: subject.config,
        notes: subject.notes,
      })),
      fidelity,
      operations,
      memory,
      ...(options.samples ? { order } : {}),
    };
  } finally {
    await Promise.allSettled(subjects.map((subject) => subject.close()));
  }

  const json = `${JSON.stringify(report, (_key, value) => (typeof value === "bigint" ? value.toString() : value), 2)}\n`;
  const outputPath = resolve(
    options.output ??
      `${HERE}/../results/node-${process.platform}-${process.arch}-node${process.versions.node.split(".")[0]}-${report.fixture.rows}rows.json`,
  );
  await mkdir(dirname(outputPath), { recursive: true });
  await writeFile(outputPath, json, "utf8");
  process.stdout.write(renderSummary(report));
  process.stdout.write(`\nwrote ${outputPath}\n`);
}

const ROUND_TRIP_UNPROVEN_PREFIXES = [
  "encoder rejected the decoded value",
  "aborted the interpreter",
  "not reached",
  "the frame could not be decoded",
];

// identical / differs / resized / unverified for one table round trip.
// `roundTrip` is recorded by the preflight; the derivation below is the
// compatibility path for reports written before that field existed.
function roundTripState(tableEntry) {
  if (tableEntry.roundTrip !== undefined) return tableEntry.roundTrip;
  if (tableEntry.sendComparable) return "identical";
  const reason = tableEntry.sendExcludedBecause ?? "";
  if (ROUND_TRIP_UNPROVEN_PREFIXES.some((prefix) => reason.startsWith(prefix))) return "unverified";
  if (tableEntry.reencodesToIdenticalQValue === false) return "differs";
  if (tableEntry.reencodesToIdenticalQValue === true) return "resized";
  return "unverified";
}


// Id of the lowest median among the subjects ranked for this operation.
function fastestSubject(entry) {
  let winner;
  for (const [id, measured] of Object.entries(entry.subjects)) {
    if (measured === undefined) continue;
    if (winner === undefined || measured.medianMs < entry.subjects[winner].medianMs) winner = id;
  }
  return winner;
}

function renderSummary(report) {
  const ids = report.subjects.map((subject) => subject.id);
  const lines = [
    `suite=node rows=${report.fixture.rows} q=${report.fixture.qVersion} node=${report.runtime.node} iterations=${report.method.iterationsPerSubjectPerOperation}`,
    "",
    `${"operation".padEnd(13)}${ids.map((id) => `${id} ms`.padStart(16)).join("")}${ids
      .slice(1)
      .map((id) => `${id} vs xqdb`.padStart(18))
      .join("")}`,
  ];
  for (const [name, entry] of Object.entries(report.operations)) {
    const winner = fastestSubject(entry);
    const medians = ids.map((id) =>
      (entry.subjects[id] === undefined
        ? "n/a"
        : `${entry.subjects[id].medianMs.toFixed(3)}${id === winner ? " *" : "  "}`
      ).padStart(16),
    );
    const ratios = ids
      .slice(1)
      .map((id) => (entry.medianRatioVsXqdb[id] === undefined ? "excluded" : `${entry.medianRatioVsXqdb[id].toFixed(2)}x`).padStart(18));
    lines.push(`${name.padEnd(13)}${medians.join("")}${ratios.join("")}`);
  }
  lines.push("* = fastest measured client for that operation");
  lines.push("", "fidelity (decoded value returned to q and compared with `~`)");
  for (const id of ids) {
    const entry = report.fidelity[id];
    const tables = Object.entries(entry.tables)
      .map(([table, value]) => `${table}=${roundTripState(value)}`)
      .join(" ");
    lines.push(
      `  ${id.padEnd(7)} int64=${entry.int64Exact ? "exact" : `lossy(${entry.int64Decoded})`} nanoseconds=${
        entry.nanosecondTimestampExact ? "exact" : `lossy(${entry.nanosecondTimestampDecoded})`
      } ${tables}`,
    );
  }
  for (const [name, entry] of Object.entries(report.operations)) {
    const table = name.includes(".") ? name.slice(name.indexOf(".") + 1) : undefined;
    for (const id of entry.roundTripUnverifiedSubjects ?? []) {
      const state = table === undefined ? "unverified" : roundTripState(report.fidelity[id].tables[table]);
      const label =
        state === "differs"
          ? "decoded value differs from the fixture after a q round trip"
          : "q round trip unverified";
      lines.push(`  ${name}: ${id} ${label} - ${entry.roundTripUnverifiedReasons[id]}`);
    }
    for (const excluded of entry.unsupported ?? []) {
      lines.push(`  excluded from ${name}: ${excluded.subject} - ${excluded.reason}`);
    }
  }
  return `${lines.join("\n")}\n`;
}

export { renderSummary };

// Guarded so the summary renderer can be imported and checked without running
// a benchmark. `process.argv[1]` rather than `import.meta.main`, which needs
// Node 24 and would silently skip the run on older releases.
const entrypoint = typeof process.argv[1] === "string" ? pathToFileURL(process.argv[1]).href : undefined;
if (import.meta.url === entrypoint) {
  await main();
  process.exit(0);
}
