"""Python q IPC client comparison against a single fixed KDB-X fixture.

Subjects are measured round-robin with a rotating start position so machine and
server drift is shared instead of accumulating on one subject.

Fidelity is a preflight, not an afterthought:

* `scalar` is a value-exact latency floor, so a subject that cannot return the
  exact value is excluded from it.
* `read.<table>` decodes an identical server byte stream into each library's
  documented frame type, so every subject that can decode it is ranked. A
  subject whose value could not be proven to survive a q round trip is named in
  `roundTripUnverifiedSubjects`; that is a statement about the round trip, not a
  proven decode loss, because the encoder can be the failing half.
* `send.<table>` is only comparable when the subject's own decoded frame
  re-encodes to a q-identical value of the same canonical size; otherwise the
  subject would be timed on different work, so it is listed in `unsupported`
  with no samples, throughput, or ratio.

int64 and nanosecond-timestamp exactness are reported by the preflight rather
than timed: they are correctness claims, and timing a wrong decode against a
right one would compare different work.

The preflight runs in one subprocess per subject because a subject can abort the
interpreter rather than raise: kola 2.5.1 panics in its Rust serializer when
asked to send a frame with list columns. Subprocess probing keeps a fatal
subject from taking the whole comparison with it and pins the failure to the
exact operation that caused it.

PyKX is deliberately absent: the licence covering its bundled q runtime forbids
making performance comparisons available to third parties.
"""

from __future__ import annotations

import argparse
import gc
import importlib.metadata as metadata
import json
import os
import platform
import random
import statistics
import subprocess
import sys
import time
from pathlib import Path

SCHEMA_VERSION = 1
TABLES = {"trade": 14, "wide": 64, "depth": 5}
SCALAR_EXPRESSION = "6f*7f"
SCALAR_EXPECTED = 42.0
LONG_EXPRESSION = "9007199254740993j"
LONG_EXPECTED = 9007199254740993
TIMESTAMP_EXPRESSION = "2024.01.02D03:04:05.123456789"
TIMESTAMP_EXPECTED_NS = 1704164645123456789
COUNT_LAMBDA = "{[x]count x}"
CANONICAL_BYTES_LAMBDA = "{[x]count -8!x}"


# ── subjects ────────────────────────────────────────────────────────────────


class Subject:
    """One measured client, normalised to a single small protocol."""

    def __init__(self, subject_id, package, frame, implementation, representation, notes=()):
        self.id = subject_id
        self.package = package
        self.version = metadata.version(package)
        self.frame = frame
        self.implementation = implementation
        self.representation = representation
        self.notes = list(notes)
        self.connection = None

    def connect(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def eval(self, expression):
        raise NotImplementedError

    def apply(self, lambda_text, value):
        raise NotImplementedError

    def read(self, table):
        return self.eval(table)

    def shape_of(self, value):
        shape = getattr(value, "shape", None)
        if isinstance(shape, tuple) and len(shape) == 2:
            return {"rows": int(shape[0]), "columns": int(shape[1])}
        return None

    def describe(self):
        return {
            "id": self.id,
            "package": self.package,
            "version": self.version,
            "frame": self.frame,
            "implementation": self.implementation,
            "representation": self.representation,
            "notes": self.notes,
        }


class XqdbSubject(Subject):
    def __init__(self, backend, host, port):
        super().__init__(
            subject_id=f"xqdb-{backend}",
            package="xqdb",
            frame=backend,
            implementation="Rust core via PyO3, Arrow C Stream decode",
            representation=f"narwhals.DataFrame over a {backend} frame",
        )
        self.backend = backend
        self.host = host
        self.port = port

    def connect(self):
        import xqdb

        self.connection = xqdb.Q(self.host, self.port, timeout=120, backend=self.backend)
        self.connection.connect()

    def close(self):
        if self.connection is not None:
            self.connection.disconnect()

    def eval(self, expression):
        return self.connection.sync(expression)

    def apply(self, lambda_text, value):
        return self.connection.sync(lambda_text, value)


class KolaSubject(Subject):
    def __init__(self, host, port):
        super().__init__(
            subject_id="kola",
            package="kola",
            frame="polars",
            implementation="Rust core via PyO3, Polars decode",
            representation="polars.DataFrame",
        )
        self.host = host
        self.port = port

    def connect(self):
        import kola

        self.connection = kola.Q(self.host, self.port)
        self.connection.connect()

    def close(self):
        if self.connection is not None:
            self.connection.disconnect()

    def eval(self, expression):
        return self.connection.sync(expression)

    def apply(self, lambda_text, value):
        return self.connection.sync(lambda_text, value)


class QconnectSubject(Subject):
    def __init__(self, host, port):
        super().__init__(
            subject_id="qconnect",
            package="qconnect",
            frame="pandas",
            implementation="pure Python codec over numpy (maintained qPython fork)",
            representation="pandas.DataFrame",
            notes=[
                "TLS is disabled for parity with the other subjects; qconnect enables it by default",
                "requests is imported at module scope but is not declared as a dependency",
            ],
        )
        self.host = host
        self.port = port

    def connect(self):
        from qconnect import qconnection

        self.connection = qconnection.QConnection(
            host=self.host, port=self.port, pandas=True, tls_enabled=False
        )
        self.connection.open()

    def close(self):
        if self.connection is not None:
            self.connection.close()

    def eval(self, expression):
        return self.connection.sendSync(expression)

    def apply(self, lambda_text, value):
        return self.connection.sendSync(lambda_text, value)


SUBJECT_BUILDERS = {
    "xqdb-pyarrow": lambda host, port: XqdbSubject("pyarrow", host, port),
    "xqdb-polars": lambda host, port: XqdbSubject("polars", host, port),
    "xqdb-pandas": lambda host, port: XqdbSubject("pandas", host, port),
    "kola": KolaSubject,
    "qconnect": QconnectSubject,
}
# The first entry is the reference subject every ratio is taken against.
SUBJECT_IDS = list(SUBJECT_BUILDERS)


# ── statistics ──────────────────────────────────────────────────────────────


def nearest_rank(ascending, fraction):
    rank = round(fraction * (len(ascending) - 1))
    return ascending[max(0, min(len(ascending) - 1, rank))]


def metrics(samples_ns, payload_bytes, keep_samples):
    ascending = sorted(samples_ns)
    median_ms = statistics.median(ascending) / 1_000_000
    report = {
        "iterations": len(samples_ns),
        "minMs": ascending[0] / 1_000_000,
        "medianMs": median_ms,
        "meanMs": statistics.fmean(samples_ns) / 1_000_000,
        "p90Ms": nearest_rank(ascending, 0.90) / 1_000_000,
        "p99Ms": nearest_rank(ascending, 0.99) / 1_000_000,
        "maxMs": ascending[-1] / 1_000_000,
    }
    if payload_bytes is not None:
        report["payloadBytes"] = payload_bytes
        report["medianMibPerSecond"] = payload_bytes / (median_ms / 1000) / 2**20
    if keep_samples:
        report["samplesMs"] = [sample / 1_000_000 for sample in samples_ns]
    return report


def rotate(items, offset):
    """Only for untimed ordering: a cyclic shift keeps neighbours fixed."""
    shift = offset % len(items)
    return [*items[shift:], *items[:shift]]


def shuffled(items, seed, round_index):
    """Deterministic per-round order.

    A cyclic rotation is not good enough here. Rotating by one preserves the
    adjacency relation, so every subject keeps the same predecessor in every
    round, and a subject that follows an expensive neighbour pays for it in
    every sample. Reshuffling varies predecessors as well as positions.
    """
    order = list(items)
    random.Random(f"{seed}:{round_index}").shuffle(order)
    return order


# ── progress ────────────────────────────────────────────────────────────────


class Progress:
    """Progress on stderr so stdout stays the summary and the probe protocol.

    A full run is minutes long and a single subject can hold a round for over a
    second, so silence is indistinguishable from a hang. A terminal gets a
    rewritten line per round; a captured log gets one line every few seconds so
    it stays readable.
    """

    THROTTLE_SECONDS = 3.0

    def __init__(self, stream=sys.stderr):
        self.stream = stream
        self.interactive = stream.isatty()
        self.pending = False
        self.last_step = 0.0

    def _write(self, text, transient):
        if transient and self.interactive:
            self.stream.write("\r" + text.ljust(96)[:96])
            self.pending = True
        else:
            if self.pending:
                self.stream.write("\n")
                self.pending = False
            self.stream.write(text + "\n")
        self.stream.flush()

    def line(self, text):
        self.last_step = 0.0
        self._write(text, transient=False)

    def step(self, text, force=False):
        now = time.monotonic()
        if not self.interactive and not force and now - self.last_step < self.THROTTLE_SECONDS:
            return
        self.last_step = now
        self._write(text, transient=True)


def format_duration(seconds):
    if seconds < 90:
        return f"{seconds:.0f}s"
    return f"{int(seconds) // 60}m{int(seconds) % 60:02d}s"


# ── measurement ─────────────────────────────────────────────────────────────


def run_operation(ids, operations, warmups, iterations, seed, check=None, progress=None, label=""):
    for round_index in range(warmups):
        if progress is not None:
            progress.step(f"{label} warmup {round_index + 1}/{warmups}")
        for subject_id in shuffled(ids, seed, -1 - round_index):
            operations[subject_id]()

    started_run = time.perf_counter()

    samples = {subject_id: [] for subject_id in ids}
    order = []
    for round_index in range(iterations):
        sequence = shuffled(ids, seed, round_index)
        order.append(sequence)
        for subject_id in sequence:
            started = time.perf_counter_ns()
            value = operations[subject_id]()
            elapsed = time.perf_counter_ns() - started
            if check is not None:
                check(subject_id, value)
            samples[subject_id].append(elapsed)
            # CPython frees on rebind, so leaving `value` bound would make the
            # next subject's timed interval pay for destroying this subject's
            # frame. Release it here, between the timers.
            del value
        if progress is not None:
            elapsed = time.perf_counter() - started_run
            remaining = elapsed / (round_index + 1) * (iterations - round_index - 1)
            progress.step(
                f"{label} {round_index + 1}/{iterations} rounds, "
                f"{format_duration(elapsed)} elapsed, {format_duration(remaining)} left",
                force=round_index in (0, iterations - 1),
            )
    return samples, order


def retained_memory(count, operation):
    import psutil

    process = psutil.Process()
    gc.collect()
    before = process.memory_info().rss
    retained = [operation() for _ in range(count)]
    gc.collect()
    after = process.memory_info().rss
    assert len(retained) == count
    retained.clear()
    gc.collect()
    return {"retainedResults": count, "deltaBytes": {"rss": after - before}}


# ── preflight, run in a subprocess per subject ──────────────────────────────


def outcome(operation):
    try:
        return {"value": operation(), "error": None}
    except Exception as error:  # noqa: BLE001 - the failure mode is the result
        return {"value": None, "error": f"{type(error).__name__}: {error}"}


def timestamp_nanoseconds(value):
    """Best-effort nanoseconds since the epoch for any subject's temporal type."""
    raw = getattr(value, "raw", value)  # qconnect wraps numpy datetime64 in QTemporal
    if getattr(raw, "dtype", None) is not None and hasattr(raw, "astype"):
        return int(raw.astype("datetime64[ns]").astype("int64"))
    if hasattr(raw, "timestamp"):  # datetime: microsecond resolution at best
        return round(raw.timestamp() * 1_000_000) * 1_000
    if isinstance(raw, int):
        return raw
    return None


def emit(event):
    sys.stdout.write(json.dumps(event) + "\n")
    sys.stdout.flush()


def probe_subject(subject_id, host, port):
    """Emit one JSON line per completed check so a fatal abort stays attributable."""
    subject = SUBJECT_BUILDERS[subject_id](host, port)
    subject.connect()

    scalar = outcome(lambda: subject.eval(SCALAR_EXPRESSION))
    long_value = outcome(lambda: subject.eval(LONG_EXPRESSION))
    timestamp = outcome(lambda: subject.eval(TIMESTAMP_EXPRESSION))
    decoded_nanos = None if timestamp["error"] else timestamp_nanoseconds(timestamp["value"])
    emit(
        {
            "event": "scalars",
            "version": subject.version,
            "scalarExact": scalar["error"] is None and float(scalar["value"]) == SCALAR_EXPECTED,
            "scalarDetail": scalar["error"] or repr(scalar["value"]),
            "int64Exact": long_value["error"] is None and int(long_value["value"]) == LONG_EXPECTED,
            "int64Detail": long_value["error"] or str(int(long_value["value"])),
            "nanosecondTimestamp": (
                "rejected"
                if timestamp["error"]
                else "exact"
                if decoded_nanos == TIMESTAMP_EXPECTED_NS
                else "lossy"
            ),
            "nanosecondTimestampDetail": timestamp["error"] or str(decoded_nanos),
        }
    )

    canonical = {table: int(subject.eval(f"count -8!{table}")) for table in TABLES}
    for table, columns in TABLES.items():
        read = outcome(lambda table=table: subject.read(table))
        if read["error"]:
            emit({"event": "read", "table": table, "readable": False, "detail": read["error"]})
            continue
        shape = subject.shape_of(read["value"])
        emit(
            {
                "event": "read",
                "table": table,
                "readable": True,
                "shape": shape,
                "shapeMatchesFixture": shape is not None and shape["columns"] == columns,
            }
        )

        value = read["value"]
        identical = outcome(lambda t=table, v=value: subject.apply(f"{{[x]{t}~x}}", v))
        if identical["error"]:
            emit(
                {
                    "event": "send",
                    "table": table,
                    "sendComparable": False,
                    "detail": f"encoder rejected the decoded frame: {identical['error']}",
                }
            )
            continue
        reencoded_bytes = int(subject.apply(CANONICAL_BYTES_LAMBDA, value))
        reencoded_rows = int(subject.apply(COUNT_LAMBDA, value))
        comparable = bool(identical["value"]) and reencoded_bytes == canonical[table]
        detail = None
        if not bool(identical["value"]):
            detail = "decoded frame does not re-encode to a q-identical value"
        elif reencoded_bytes != canonical[table]:
            detail = (
                f"re-encoded canonical size {reencoded_bytes} != fixture {canonical[table]}"
            )
        emit(
            {
                "event": "send",
                "table": table,
                "sendComparable": comparable,
                "reencodesToIdenticalQValue": bool(identical["value"]),
                "reencodedCanonicalBytes": reencoded_bytes,
                "reencodedRows": reencoded_rows,
                "detail": detail,
            }
        )

    emit({"event": "done"})
    subject.close()


def discover(subject_id, host, port):
    """Run the preflight for one subject out-of-process and fold it into a report."""
    completed = subprocess.run(  # noqa: S603 - fixed argv, no shell
        [sys.executable, str(Path(__file__).resolve()), "--probe", subject_id, "--host", host, "--port", str(port)],
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    events = []
    for line in completed.stdout.splitlines():
        line = line.strip()
        if line.startswith("{"):
            events.append(json.loads(line))

    report = {
        "tables": {table: {"readable": False, "sendComparable": False} for table in TABLES},
        "probe": {
            "exitCode": completed.returncode,
            "completed": any(event["event"] == "done" for event in events),
        },
    }
    fatal = None
    if not report["probe"]["completed"]:
        tail = (completed.stderr or "").strip().splitlines()
        fatal = " | ".join(tail[-4:]) or f"probe exited with code {completed.returncode}"
        report["probe"]["fatal"] = fatal

    reached = None
    for event in events:
        if event["event"] == "scalars":
            report.update({key: value for key, value in event.items() if key != "event"})
        elif event["event"] == "read":
            reached = ("read", event["table"])
            entry = report["tables"][event["table"]]
            entry["readable"] = event["readable"]
            entry["shape"] = event.get("shape")
            entry["shapeMatchesFixture"] = event.get("shapeMatchesFixture", False)
            if not event["readable"]:
                entry["readExcludedBecause"] = event["detail"]
                entry["sendExcludedBecause"] = "the frame could not be decoded"
        elif event["event"] == "send":
            reached = ("send", event["table"])
            entry = report["tables"][event["table"]]
            entry["sendComparable"] = event["sendComparable"]
            entry["reencodesToIdenticalQValue"] = event.get("reencodesToIdenticalQValue", False)
            entry["reencodedCanonicalBytes"] = event.get("reencodedCanonicalBytes")
            entry["reencodedRows"] = event.get("reencodedRows")
            if not event["sendComparable"]:
                entry["sendExcludedBecause"] = event["detail"]

    if fatal is not None:
        # The probe died: the operation after the last emitted event is the culprit.
        pending = [
            (stage, table)
            for table in TABLES
            for stage in ("read", "send")
        ]
        start = pending.index(reached) + 1 if reached in pending else 0
        for index, (stage, table) in enumerate(pending):
            if index < start:
                continue
            entry = report["tables"][table]
            if stage == "read":
                entry["readable"] = False
                entry["readExcludedBecause"] = (
                    f"aborted the interpreter during read: {fatal}" if index == start else "not reached: an earlier operation aborted the interpreter"
                )
            entry["sendComparable"] = False
            entry["sendExcludedBecause"] = (
                f"aborted the interpreter during send: {fatal}"
                if index == start and stage == "send"
                else entry.get("sendExcludedBecause")
                or "not reached: an earlier operation aborted the interpreter"
            )
    return report


# ── cli ─────────────────────────────────────────────────────────────────────


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default=os.environ.get("XQDB_TEST_Q_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("XQDB_TEST_Q_PORT", "1801")))
    parser.add_argument("--warmups", type=int, default=int(os.environ.get("XQDB_BENCH_WARMUPS", "3")))
    parser.add_argument(
        "--iterations", type=int, default=int(os.environ.get("XQDB_BENCH_ITERATIONS", "50"))
    )
    parser.add_argument(
        "--memory-results", type=int, default=int(os.environ.get("XQDB_BENCH_MEMORY_RESULTS", "5"))
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=int(os.environ.get("XQDB_BENCH_SEED", "42")),
        help="seed for the deterministic per-round subject order",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--samples", action="store_true", help="keep every raw duration")
    parser.add_argument(
        "--probe", choices=SUBJECT_IDS, help="internal: emit the preflight for one subject"
    )
    args = parser.parse_args()
    if args.warmups < 0:
        parser.error("--warmups must be non-negative")
    if args.iterations < 1:
        parser.error("--iterations must be positive")
    if args.memory_results < 1:
        parser.error("--memory-results must be positive")
    return args


def render_summary(report):
    ids = [subject["id"] for subject in report["subjects"]]
    reference = report["method"]["referenceSubject"]
    lines = [
        f"suite=python rows={report['fixture']['rows']} q={report['fixture']['qVersion']} "
        f"python={report['runtime']['python']} "
        f"iterations={report['method']['iterationsPerSubjectPerOperation']}",
        "",
        "operation".ljust(13) + "".join(f"{subject_id} ms".rjust(17) for subject_id in ids),
    ]
    for name, entry in report["operations"].items():
        cells = [
            (
                "n/a"
                if entry["subjects"].get(subject_id) is None
                else f"{entry['subjects'][subject_id]['medianMs']:.3f}"
            ).rjust(17)
            for subject_id in ids
        ]
        lines.append(name.ljust(13) + "".join(cells))
    lines += ["", f"median vs {reference}; (x) = vs the xqdb subject returning the same frame type"]
    for name, entry in report["operations"].items():
        parts = []
        for subject_id in ids:
            if subject_id == reference:
                continue
            ratio = entry["medianRatioVsReference"].get(subject_id)
            peer = entry["medianRatioVsSameFrameXqdb"].get(subject_id)
            if ratio is None:
                parts.append(f"{subject_id}=excluded")
            elif peer is None or subject_id.startswith("xqdb-"):
                parts.append(f"{subject_id}={ratio:.2f}x")
            else:
                parts.append(f"{subject_id}={ratio:.2f}x ({peer:.2f}x)")
        lines.append(f"  {name.ljust(12)} " + "  ".join(parts))
    lines += ["", "fidelity (decoded frame returned to q and compared with `~`)"]
    for subject_id in ids:
        entry = report["fidelity"][subject_id]
        tables = " ".join(
            f"{table}={'identical' if value['sendComparable'] else 'differs'}"
            for table, value in entry["tables"].items()
        )
        lines.append(
            f"  {subject_id.ljust(14)} int64={'exact' if entry.get('int64Exact') else 'lossy'} "
            f"nanoseconds={entry.get('nanosecondTimestamp', 'unknown')} {tables}"
        )
    for name, entry in report["operations"].items():
        for subject_id in entry.get("roundTripUnverifiedSubjects", []):
            reason = entry["roundTripUnverifiedReasons"].get(subject_id, "unknown")
            lines.append(f"  {name}: {subject_id} q round trip unverified - {reason}")
        for excluded in entry.get("unsupported", []):
            lines.append(f"  excluded from {name}: {excluded['subject']} - {excluded['reason']}")
    return "\n".join(lines) + "\n"


def main():
    args = parse_args()
    if args.probe is not None:
        probe_subject(args.probe, args.host, args.port)
        return

    progress = Progress()
    total_operations = 1 + 2 * len(TABLES)
    fidelity = {}
    for index, subject_id in enumerate(SUBJECT_IDS, start=1):
        progress.step(f"[preflight {index}/{len(SUBJECT_IDS)}] {subject_id}")
        fidelity[subject_id] = discover(subject_id, args.host, args.port)
        probe = fidelity[subject_id]["probe"]
        if not probe["completed"]:
            progress.line(f"[preflight {index}/{len(SUBJECT_IDS)}] {subject_id}: aborted, capabilities reduced")
    progress.line(f"[preflight] {len(SUBJECT_IDS)} subjects probed")

    subjects = [SUBJECT_BUILDERS[subject_id](args.host, args.port) for subject_id in SUBJECT_IDS]
    by_id = {subject.id: subject for subject in subjects}
    all_ids = [subject.id for subject in subjects]
    reference = subjects[0]
    # kola returns Polars and qconnect returns pandas, so the fairest single
    # number for each is the xqdb subject that materialises the same frame type.
    same_frame_reference = {
        subject.id: next(
            other.id
            for other in subjects
            if other.package == reference.package and other.frame == subject.frame
        )
        for subject in subjects
    }

    try:
        for subject in subjects:
            subject.connect()

        fixture = {
            "host": args.host,
            "port": args.port,
            "qVersion": int(reference.eval(".z.K")),
            "rows": int(reference.eval(".xqdb.rows")),
            "seed": int(reference.eval(".xqdb.seed")),
            "tables": {},
        }
        for table, columns in TABLES.items():
            fixture["tables"][table] = {
                "columns": columns,
                "canonicalBytes": int(reference.eval(f"count -8!{table}")),
            }
        # Every subject must agree on what it is talking to before anything is timed.
        for subject in subjects:
            assert int(subject.eval(".xqdb.rows")) == fixture["rows"], f"{subject.id}: row mismatch"
            assert int(subject.eval(".z.K")) == fixture["qVersion"], f"{subject.id}: q mismatch"
            assert int(subject.eval(".xqdb.seed")) == fixture["seed"], f"{subject.id}: seed mismatch"
        for subject_id, entry in fidelity.items():
            for table, table_entry in entry["tables"].items():
                if table_entry.get("shape") is not None:
                    assert table_entry["shape"]["rows"] == fixture["rows"], (
                        f"{subject_id}: {table} preflight saw a different fixture"
                    )

        operations = {}
        order = {}

        def record(name, ids, payload_bytes, build, check=None, extra=None):
            record.index += 1
            label = f"[bench {record.index}/{total_operations}] {name} ({len(ids)} subjects)"
            progress.step(label)
            started_label = time.perf_counter()
            samples, sequence = run_operation(
                ids,
                {subject_id: build(by_id[subject_id]) for subject_id in ids},
                args.warmups,
                args.iterations,
                args.seed,
                check,
                progress,
                label,
            )
            per_subject = {
                subject_id: metrics(samples[subject_id], payload_bytes, args.samples)
                for subject_id in ids
            }
            reference_median = per_subject[reference.id]["medianMs"]
            operations[name] = {
                "payloadBytes": payload_bytes,
                "subjects": per_subject,
                "medianRatioVsReference": {
                    subject_id: per_subject[subject_id]["medianMs"] / reference_median
                    for subject_id in ids
                },
                "medianRatioVsSameFrameXqdb": {
                    subject_id: per_subject[subject_id]["medianMs"]
                    / per_subject[same_frame_reference[subject_id]]["medianMs"]
                    for subject_id in ids
                    if same_frame_reference[subject_id] in per_subject
                },
                **(extra or {}),
            }
            if args.samples:
                order[name] = sequence
            progress.line(
                f"{label} done in {format_duration(time.perf_counter() - started_label)}: "
                + "  ".join(
                    f"{subject_id}={per_subject[subject_id]['medianMs']:.2f}ms" for subject_id in ids
                )
            )

        record.index = 0

        scalar_ids = [subject_id for subject_id in all_ids if fidelity[subject_id].get("scalarExact")]
        assert reference.id in scalar_ids, "reference subject failed the scalar preflight"
        record(
            "scalar",
            scalar_ids,
            None,
            lambda subject: lambda: subject.eval(SCALAR_EXPRESSION),
            extra={
                "unsupported": [
                    {
                        "subject": subject_id,
                        "reason": f"scalar decode is not exact: {fidelity[subject_id].get('scalarDetail')}",
                    }
                    for subject_id in all_ids
                    if subject_id not in scalar_ids
                ]
            },
        )

        for table in TABLES:
            payload_bytes = fixture["tables"][table]["canonicalBytes"]
            table_fidelity = {subject_id: fidelity[subject_id]["tables"][table] for subject_id in all_ids}

            def check_read(subject_id, value, table=table):
                shape = by_id[subject_id].shape_of(value)
                assert shape is not None and shape["rows"] == fixture["rows"], (
                    f"{subject_id}: {table} row count mismatch"
                )

            readable = [subject_id for subject_id in all_ids if table_fidelity[subject_id]["readable"]]
            assert reference.id in readable, f"{table}: reference subject cannot decode the table"
            record(
                f"read.{table}",
                readable,
                payload_bytes,
                lambda subject, table=table: lambda: subject.read(table),
                check_read,
                extra={
                    "roundTripUnverifiedSubjects": [
                        subject_id
                        for subject_id in readable
                        if not table_fidelity[subject_id].get("reencodesToIdenticalQValue")
                    ],
                    "roundTripUnverifiedReasons": {
                        subject_id: table_fidelity[subject_id].get("sendExcludedBecause", "unknown")
                        for subject_id in readable
                        if not table_fidelity[subject_id].get("reencodesToIdenticalQValue")
                    },
                    "unsupported": [
                        {
                            "subject": subject_id,
                            "reason": table_fidelity[subject_id].get("readExcludedBecause", "unknown"),
                        }
                        for subject_id in all_ids
                        if subject_id not in readable
                    ],
                },
            )

            sendable = [
                subject_id for subject_id in all_ids if table_fidelity[subject_id]["sendComparable"]
            ]
            assert reference.id in sendable, f"{table}: reference subject cannot send comparably"
            decoded = {subject_id: by_id[subject_id].read(table) for subject_id in sendable}

            def check_send(subject_id, value, table=table):
                assert int(value) == fixture["rows"], f"{subject_id}: {table} send count mismatch"

            record(
                f"send.{table}",
                sendable,
                payload_bytes,
                lambda subject: lambda: subject.apply(COUNT_LAMBDA, decoded[subject.id]),
                check_send,
                extra={
                    "unsupported": [
                        {
                            "subject": subject_id,
                            "reason": table_fidelity[subject_id].get("sendExcludedBecause", "unknown"),
                        }
                        for subject_id in all_ids
                        if subject_id not in sendable
                    ]
                },
            )

        memory = {}
        for table_index, table in enumerate(TABLES):
            memory[table] = {}
            readable = [
                subject_id
                for subject_id in rotate(all_ids, table_index)
                if fidelity[subject_id]["tables"][table]["readable"]
            ]
            for subject_id in readable:
                progress.step(f"[memory {table_index + 1}/{len(TABLES)}] {table} {subject_id}")
                memory[table][subject_id] = retained_memory(
                    args.memory_results,
                    lambda subject_id=subject_id, table=table: by_id[subject_id].read(table),
                )
        progress.line(f"[memory] {len(TABLES)} tables probed")

        report = {
            "schemaVersion": SCHEMA_VERSION,
            "suite": "python",
            "generatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "fixture": fixture,
            "runtime": {
                "python": platform.python_version(),
                "platform": platform.platform(),
                "machine": platform.machine(),
                "cpu": platform.processor(),
                "packages": {
                    name: metadata.version(name)
                    for name in ("narwhals", "pyarrow", "pandas", "polars", "numpy")
                },
            },
            "method": {
                "warmupsPerSubjectPerOperation": args.warmups,
                "iterationsPerSubjectPerOperation": args.iterations,
                "retainedResultsPerMemoryProbe": args.memory_results,
                "clock": "time.perf_counter_ns",
                "referenceSubject": reference.id,
                "sameFrameReference": same_frame_reference,
                "orderSeed": args.seed,
                "scheduling": (
                    "one request in flight per subject; every round runs each subject once in a "
                    "deterministic order reshuffled from `orderSeed`, so positions and "
                    "predecessors both vary; a cyclic rotation would pin every subject behind the "
                    "same neighbour in every round"
                ),
                "percentiles": "nearest-rank on raw durations",
                "payloadBytes": (
                    "server-side `count -8!table` for the fixture table: a logical payload size "
                    "shared by all subjects, not observed wire bytes"
                ),
                "throughput": "payloadBytes divided by the median duration",
                "memory": (
                    "process-wide RSS delta around retained decoded frames with forced GC at both "
                    "snapshots; a noisy diagnostic, not a library footprint"
                ),
                "preflight": (
                    "one subprocess per subject emits a JSON line per completed check, so a "
                    "subject that aborts the interpreter is attributed to the exact operation "
                    "that killed it instead of ending the comparison"
                ),
                "scalarComparability": (
                    "value-exact latency floor; a subject that cannot return the exact value is "
                    "listed in `unsupported` instead of being ranked"
                ),
                "readComparability": (
                    "every subject that can decode the table is ranked, because the server sends "
                    "identical bytes to all of them; proven decode losses are the int64 and "
                    "nanosecond checks in `fidelity`, while `roundTripUnverifiedSubjects` records "
                    "only that the decode-then-encode round trip could not be proven, which the "
                    "encoder alone can cause"
                ),
                "sendComparability": (
                    "sends are compared only for subjects whose decoded frame re-encodes to a "
                    "q-identical value of the same canonical size; others are listed in "
                    "`unsupported` with no samples, throughput, or ratio"
                ),
                "untimedCorrectnessChecks": (
                    "int64 and nanosecond-timestamp exactness are reported by the preflight, not "
                    "timed: timing a wrong decode against a right one would compare different work"
                ),
                "excludedSubjects": {
                    "pykx": (
                        "the licence covering PyKX's bundled q runtime forbids making performance "
                        "comparisons available to third parties"
                    ),
                    "qpython and qpython3": (
                        "both dereference numpy aliases removed in numpy 2, so they require "
                        "numpy<1.20 and Python<=3.9; qconnect is the maintained fork measured here"
                    ),
                    "pyq": "embeds Python inside q rather than acting as a client",
                },
            },
            "subjects": [subject.describe() for subject in subjects],
            "fidelity": fidelity,
            "operations": operations,
            "memory": memory,
            **({"order": order} if args.samples else {}),
        }
    finally:
        for subject in subjects:
            try:
                subject.close()
            except Exception as error:  # noqa: BLE001 - teardown must not mask a real failure
                print(f"warning: {subject.id} teardown failed: {error}")

    output = args.output or (
        Path(__file__).resolve().parent.parent
        / "results"
        / (
            f"python-{platform.system().lower()}-{platform.machine().lower()}"
            f"-py{'.'.join(platform.python_version_tuple()[:2])}-{fixture['rows']}rows.json"
        )
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(render_summary(report))
    print(f"wrote {output}")


if __name__ == "__main__":
    main()
