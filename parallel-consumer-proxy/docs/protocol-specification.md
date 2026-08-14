# Parallel Consumer Proxy - Protocol Specification (v1, FROZEN)

This document and `parallel-consumer-proxy-protocol/src/main/proto/parallelconsumer/proxy/v1/proxy.proto`
together are the complete contract between the sidecar proxy and a client library. A client author works
from these two artifacts alone - **never from the proxy's Java source**. If implementing a client requires
reading Java, that is a defect in this document; file it against astubbs#242.

The schema is **frozen**. It was completed on the evidence of an end-to-end spike (one record through both
Java transports, the provisional schema carried the whole slice unchanged) plus the specified scenarios of
the engine's lease/reconnect, terminal-failure and lifecycle work. After this freeze the wire may only
**gain**, never change:

- Fields and messages may be **added**; nothing may be removed, renumbered, retyped or renamed. CI enforces
  this with `buf breaking` in the `FILE` category (`bin/check-proto-breaking.sh`) - `FILE` is deliberate,
  because the weaker `WIRE` category would permit source-breaking changes the compatibility promise forbids.
- Every addition ships with a **capability token** (see [Capabilities](#capabilities-and-versioning)), so it
  is negotiated in the handshake rather than inferred from field presence.
- Every addition ships with a note naming which client libraries must be revisited.

A schema-coverage test (`SpecificationCoverageTest` in the proxy module) fails the build when a message,
field, enum or enum value in the frozen `.proto` is missing from this document - the specification-to-schema
diff is enforced empty, not audited by hand.

## Wire contract at a glance

- **Transport:** gRPC over HTTP/2, plaintext, loopback. One service, one method:
  `parallelconsumer.proxy.v1.ProxyService/Session`, a single bidirectional stream of `ClientMessage`
  (client→proxy) and `ProxyMessage` (proxy→client).
- **Serialization:** protobuf (proto3) per the frozen `.proto`. Every scalar field carries explicit
  presence (`optional`), deliberately: presence-vs-absence is meaningful throughout, and explicit presence
  is frozen now because retrofitting it later is a source break in several generated languages.
- **Golden bytes:** `parallel-consumer-proxy-protocol/src/test/resources/bz/stub/parallelconsumer/proxy/protocol/`
  holds `golden-client-messages.bin` and `golden-proxy-messages.bin` - one canonical scripted session,
  length-delimited (standard varint-prefixed) messages, covering every message type, a beyond-int32 epoch, a
  tombstone, and presence/absence in both directions. Every language's generated parser must read those
  bytes to exactly the documented values; they are the cross-language fixed point a same-runtime round trip
  cannot provide. The exact contents: **eight** `ClientMessage`s (configure, heartbeat, three reports,
  worker-died, manifest, released-report) and **five** `ProxyMessage`s (configured, dispatch, drop,
  shutdown, set-executor-count) - the transcript's reconnect `Configured` is byte-identical to the first
  and is deliberately not committed twice. The corpus pins **serialization, not session legality**: it
  declares `capabilities: ["dispatch"]` yet carries traffic from every token (and a `SetExecutorCount` no
  v1 proxy ever sends), and it reports two outcomes at one epoch - a parser fixture, never a model session
  to replay against a live proxy.

### Deliberate narrowness

The protocol uses a deliberately narrow slice of gRPC, and this narrowness is a **constraint on the
protocol**, not an observation: a feature only the official gRPC implementations support would silently
shrink the set of languages that can implement a client. Confirm your language's gRPC library suffices by
checking this list and nothing more:

- one bidirectional streaming call, held open for the session's lifetime;
- the client declares an ordinary target authority (host:port); no other metadata is required;
- **no** TLS, **no** authentication, **no** interceptors required client-side, **no** load balancing,
  **no** xDS, **no** retry policy, **no** per-call deadline negotiation, **no** compression requirements.

## Connection model

### Process and lifecycle channel

The application (via its client library) starts the proxy as a **child process**, launched **directly, not
through a shell** - the proxy detects parent death by EOF on its inherited stdin, and a wrapper process
holding the write end defeats that signal. The parent holds the stdin pipe's write end and never writes;
proxy exit on parent death is the proxy's job, not the client's.

The proxy binds an ephemeral port on the loopback interface and reports it as **the first line on its
stdout**, exactly:

```
port: <n>
```

The client reads that line, connects to `127.0.0.1:<n>` plaintext, and opens the one `Session` stream.
Nothing else is ever written to the lifecycle channel before the port line.

### Admission

- The proxy serves **exactly one** client connection at a time. A second concurrent `Session` is refused
  with gRPC status `RESOURCE_EXHAUSTED`. A reconnect re-uses the freed slot; it does not need a second one.
- The proxy rejects any connection whose declared target authority is not in its allowlist with
  `PERMISSION_DENIED`, before any message is processed. The allowlist defaults to the loopback host forms
  and the configured bind address; a connection declaring no authority is accepted. (The threat is a browser
  page reaching the loopback listener cross-origin.)
- v1 has **no listener authentication**. `Configure.launch_token` is reserved for the post-v6 admission
  hardening (a per-launch secret handed down the lifecycle pipe); v1 clients omit it and a v1 proxy ignores
  it.

### Handshake

The first client message on a stream determines its kind:

- **Fresh session:** first message is `Configure`. The proxy builds its Kafka clients and engine from it,
  subscribes, and replies with `Configured` - the *effective* values it is running with. Only after
  `Configured` arrives may the client treat the session as open.
- **Reconnect:** first message is `Manifest` (see [Connection loss](#connection-loss-the-window-and-the-manifest)).
  The proxy reconciles, replies with the **unchanged** effective `Configured` (so every stream's handshake
  ends the same way), then sends any `Drop`s and resumes dispatching.
- Anything else first is a protocol violation: the stream is closed with `FAILED_PRECONDITION`. The
  admission slot is released; a corrected client may connect again.

Configuration is **connect-time only** (it travels on the stream and nowhere else - the proxy reads no
config file, no environment variable, no shell) and the subscription is fixed for the process lifetime. A
second `Configure` on an already-configured stream is refused by re-sending the original effective
`Configured` unchanged - a truthful refusal: the client reads back a configuration that is not what it just
asked for. A `Configure` on a **new** stream while the proxy is already configured is refused with
`FAILED_PRECONDITION` (the subscription cannot change until the process is replaced).

## The dispatch / report cycle

1. The engine selects records under its configured ordering and coalesces them into a **wave**: one
   `Dispatch` message carrying one or more `DispatchRecord`s. Under `PARTITION` or `KEY` ordering, the
   records of one wave are drawn from distinct shards; under `UNORDERED` there is no shard constraint.
2. The client's admin thread reads the wave and queues each record, in wave order, for its executors (the
   queue's rules are the client-authoring guide's dispatch-queue section - normative for every client).
3. An executor runs the user's function on the record and reports **per record**, in any order, with the
   record's `Token` echoed **verbatim**. Reports for different records need not follow dispatch order
   (out-of-order completion is the product's point).
4. The proxy applies the outcome: success (producing any payload first), failure into retry scheduling,
   terminal resolution, or released back to scheduling. Offsets advance per the engine's offset tracking;
   out-of-order completions are encoded in commit metadata and survive restarts.

**Flow control does not exist on the wire, deliberately.** The proxy never dispatches more than
`max_concurrency` records at once (its own in-flight ceiling); the client never signals capacity. There are
no credits, no acks, no window updates. A client that cannot keep up simply reports more slowly, and the
in-flight ceiling does the rest.

### The epoch echo rule (fencing)

Every delivery carries a `Token`: `(record_id, epoch)`. The epoch is the engine's delivery count for that
record, **captured at dispatch** - an `int64`, because its source is a 64-bit counter. The rules, absolute:

- The client treats the token as **opaque**. It must not parse `record_id`, compare epochs, or derive
  anything from either field. It stores nothing per record: the token rides from dispatch to report on the
  executor's stack (or your language's equivalent) and is echoed byte-identically.
- The proxy discards a report whose token names a **superseded** epoch - a delivery that has ended (the
  record was returned, redelivered, or resolved). The live delivery is untouched. This is what makes a slow
  duplicate report from a fenced worker harmless.
- A report whose token names **no record in flight** is discarded (late duplicate or fabrication); nothing
  is disturbed.

Fencing bounds what it can: it fences **reports and Kafka-side effects**. It cannot fence a worker's
external side effects (a database write, an HTTP call) - true of any at-least-once system, and the client
must not imply otherwise to its users.

## Liveness: leases and heartbeats

Each dispatched record carries a **lease** proving the client is alive - attached at dispatch, extended for
**all** in-flight records of the session by connection-level `Heartbeat` messages. The client's admin sends
`Heartbeat` every `heartbeat_interval` (from `Configured`), unconditionally - never withheld because the
dispatch queue is full or executors are busy. This whole section is gated by the `heartbeat` capability
(see [Capabilities](#capabilities-and-versioning)): on a session that did not negotiate it, the client
sends no heartbeats, `Configured` carries no `heartbeat_interval` or `lease_duration`, and no lease
machinery runs. Heartbeating starts when `Configured` arrives (nothing earlier names the interval) and,
after a reconnect, resumes from the moment the reconnect handshake's `Configured` arrives - the same
moment reporting may resume.

- The lease is **not a processing deadline**. A worker whose function runs for hours keeps its record as
  long as the admin heartbeats. There is no per-record clock: "a record stays in flight until your function
  returns" is the product's differentiator, and a client must not build one.
- When heartbeats stop for longer than `lease_duration`, the proxy returns that session's in-flight records
  to scheduling with their attempt counts **unchanged** (no verdict was reached, so no retry is consumed).
- During a connection loss no heartbeat can arrive, so leases are **suspended** and the reconnect window
  alone governs the held records; leases resume on reconnect for the records the manifest keeps in flight.
  The lease and the window are alternatives in time, never concurrent clocks over one record.

## Connection loss, the window, and the manifest

Losing the connection does **not** return the connection's records to scheduling. The proxy holds them for
a bounded window (`reconnect_window`, default 30s) - returning them immediately is what would let two
workers run one key's code while the original worker is still alive and finishing.

On the client side, connection loss splits the in-flight set in two:

- Records **live workers hold** (executing right now): keep running; their results are reported after
  reconnecting, subject to fencing.
- Records **queued but never handed to an executor**: the client **discards them**. They are not held by
  any worker, so they must not appear in the manifest - this discard is what keeps the manifest truthful,
  and the proxy will return them to scheduling as unmanifested records.

A client reconnecting within the window opens the new stream with a `Manifest` naming the tokens its live
workers still hold. The proxy reconciles three ways, per held record:

| Manifest says | Proxy does |
|---|---|
| Token names the **current** delivery | Stays in flight, untouched; its lease resumes |
| Token names a **superseded** delivery | Proxy sends `Drop`; any later report for it is discarded |
| Record **not named** by any token | Returned to scheduling, attempt count unchanged |

A manifest token the proxy never issued is rejected without disturbing anything held. Records still
unaccounted for when the window expires are returned to scheduling, attempt counts unchanged. After
reconciliation the proxy replies with the unchanged effective `Configured`, then any `Drop`s, then resumes
dispatching; the client may report from the moment `Configured` arrives.

### Worker death

The window and the lease are backstops. The **primary** reclaim path is `WorkerDied`: the client library
can observe its own worker exiting, and reports the tokens that worker held. The proxy returns those
records to scheduling immediately, attempt counts unchanged, without waiting for anything to expire.

## Outcomes

`Report.outcome` is a oneof with four variants:

- **`Success`** - the function completed. May carry `produce`: records the proxy produces **with its own
  producer** before the input record's offset may become eligible to commit. This is the only sanctioned
  route for worker output to Kafka; workers never produce directly. The produce is at-least-once: a failed
  produce is applied as a failure so the record retries, and a redelivered worker may produce duplicates.
- **`Failure`** - the function failed; the record returns to the engine's retry scheduling, exactly as an
  in-process user function throwing. The `reason` text rides back on the redelivery
  (`last_failure_reason`), verbatim. It is worker-supplied and may embed record payload: **untrusted
  input** everywhere it is handled, and the proxy bounds and sanitises it before any log line.
- **`Terminal`** - the worker declares the record terminally failed. The proxy produces the record to the
  configured `terminal_topic` and advances the offset; the record is never dispatched again. Only valid on
  a session whose **effective** configuration carries a `terminal_topic` - which requires both the
  negotiated `terminal` capability and a `Configure` that named a topic; the client checks
  `Configured.terminal_topic`, not its own request. A `Terminal` report without one is a protocol
  violation, discarded non-fatally: the record stays in flight and a later report at the same epoch is
  still honoured. The terminal produce and the commit are not atomic: a crash between them redelivers the record
  and can duplicate the terminal entry (at-least-once, again). The terminal topic inherits the source
  topic's confidentiality expectations - readable only by the audience already entitled to the source
  topic, bounded retention - and the proxy cannot enforce broker-side ACLs, so it does not claim to.
- **`Released`** - the client returns a record it never ran (the `Shutdown` path for queued records). The
  proxy treats it exactly as an abandonment at the captured epoch: back to scheduling, attempt count
  unchanged. The client never invents a verdict for work it did not do.

**`OUTCOME_NOT_SET` is a protocol violation, settled here.** A receiver seeing a `Report` with no
recognized outcome is seeing either a client bug or an outcome variant added by a revision the handshake
did not negotiate - the capability rule forbids the latter, so there is nothing legitimate it can mean.
The proxy discards the report (the record stays in flight for the liveness machinery) and logs it; it never
maps an unset outcome onto a default. Symmetrically, a client must never send a `Report` without an
outcome.

## Shutdown and drain

Both directions exist, and both end with everything either reported or released - never abandoned, never
guessed.

**Proxy-initiated** (the operator stopping the sidecar while a client is connected): the proxy sends
`Shutdown`, stops dispatching, and drains. On `Shutdown` the client: stops handing queued records to
executors; reports every queued record `Released`; lets executing records finish and report normally; then
half-closes the stream. The proxy commits what resolved within `drain_timeout`, completes the stream, and
exits. Records unreported at the deadline are left for redelivery - the proxy does not invent an outcome.
(`Shutdown` is gated by the `shutdown` capability; a session that did not negotiate it gets the abrupt
path below, never the message.)

**Parent death is not this path.** The stdin write end is held by the client's own process, so EOF on
stdin *is* proof the client is gone - there is nobody left to read a `Shutdown`, and the proxy does not
send one. It commits what has already resolved and exits; a stream still technically open is terminated
abruptly (the peer, if any half-dead remnant is still reading, observes stream termination, not a drain
order). Uncommitted records redeliver to the next group member as an ordinary rebalance.

**Client-initiated** (the application closing its client): the client performs the same sequence unprompted
- stop hand-out, `Released` for the queue, final reports for executing records - and then **half-closes**
the stream (calls its language's "no more sends"). The half-close is the shutdown signal; there is no
shutdown request message, deliberately, because a client that has already reported or released everything
has nothing left to say. The proxy then drains, commits, completes the stream, and exits with its group
membership cleanly ended.

An application killed without running any code (SIGKILL) is the parent-death case above: the watchdog
exits the process, and the rebalance does the rest.

## Capabilities and versioning

**The capability list is the protocol's only versioning mechanism - there is no version number field, and
that is a decision, not an omission.** A numeric version invites range arithmetic ten clients would each
implement differently; a set of named tokens is compared with set intersection, which every language spells
the same way. (If a diagnostic build-version string is ever wanted, it is an additive field with its own
capability, like anything else.)

`Configure.capabilities` declares the tokens the client implements; an **empty list means the v1
baseline** (a pre-capability client gets everything v1 defines, not silence). `Configured.capabilities` is
the negotiated **intersection** of the client's declared set and the proxy's own. The rule both sides obey:
**neither side sends a message type or outcome variant whose token is outside the negotiated set.** A
message received outside the negotiated set is a protocol violation; the receiver ignores it or fails the
stream - it never acts on it.

v1 defines these tokens:

| Token | Gates |
|---|---|
| `dispatch` | `Dispatch` waves (proxy→client) |
| `heartbeat` | `Heartbeat` and the lease semantics (client→proxy) |
| `manifest` | `Manifest` reconnects and `Drop` replies |
| `worker-death` | `WorkerDied` (client→proxy) |
| `shutdown` | `Shutdown` (proxy→client) and the `Released` outcome |
| `terminal` | The `Terminal` outcome |
| `set-executor-count` | `SetExecutorCount` - **defined and never granted by a v1 proxy** |

The v1 **baseline** is the first six. A conformant v1 client implements all six; the tokens exist so a
future revision can add a seventh without a flag day, and so an older client meeting a newer proxy (or the
reverse) degrades by negotiation instead of by surprise.

Three consequences of the negotiation rule, spelled out because every one of them bit the specification
probe:

- **Duties follow the negotiated set, not this document's imperative mood.** Where a later section says
  "send `Heartbeat` unconditionally" or "reconnect with `Manifest`", the duty exists **iff its gating
  token is in the negotiated set**. A client on a session without `heartbeat` sends no heartbeats (and
  has no lease semantics to honour); without `manifest` it has no reconnect path; without `terminal` it
  may not report `Terminal`; without `shutdown` it will never receive one. A gated message sent anyway is
  the ordinary un-negotiated-message violation - the receiver ignores it or fails the stream.
- **The proxy's own token set is not promised by this specification.** A *complete* v1 proxy grants the
  whole baseline, but a proxy whose machinery for a token is not yet built excludes that token, so an
  empty (= baseline) client declaration can still negotiate down to a subset. The client reads
  `Configured.capabilities` and behaves accordingly; asserting the baseline came back is a conformance
  test's job, not a client library's assumption.
- **A reconnect stream carries no `Configure`, so it negotiates nothing of its own**: the original
  session's negotiated set survives connection loss and governs the reconnect stream. Opening a stream
  with `Manifest` is itself gated - legal only when `manifest` is in that surviving set.

## The executor count

`Configured.executor_count` tells the client how many executors to run - threads, processes, goroutines:
the mechanism is the client's, in its own language. The value is a **pure function of connect-time
configuration**, computed once, sent once, never revised. It is derived from nothing the proxy observes
about the client at runtime - no feedback loop - and clients **must not assume any particular formula**
relating it to `max_concurrency`; the formula is the proxy's to choose and may change without protocol
impact.

`SetExecutorCount` exists in the schema **declared and unused**: a v1 proxy never sends it, no v1 client
implements a listener for it, and a v1 client receiving one treats it as a protocol violation. It is
reserved so a future dynamic count is an additive change gated by the `set-executor-count` capability
rather than a breaking one.

## Errors the proxy returns

All session-fatal refusals arrive as gRPC status errors closing the stream. The admission slot is released
on any stream termination, so every refusal is recoverable by correcting and reconnecting.

| Status | When |
|---|---|
| `PERMISSION_DENIED` | Connection's declared authority not in the allowlist (closed before any message is handled) |
| `RESOURCE_EXHAUSTED` | A second concurrent connection while one session is live |
| `FAILED_PRECONDITION` | First message on the stream is neither `Configure` (fresh) nor `Manifest` (reconnect); a `Manifest`-first stream when `manifest` is not in the surviving session's negotiated set; or a `Configure` on a new stream when the proxy is already configured |
| `INVALID_ARGUMENT` | `Configure` refused: both or neither of topics/pattern; unparseable pattern; `max_concurrency < 1`; the transactional commit mode (impossible through the proxy in v1); an unrecognized enum value; or Kafka clients rejecting the property map |
| `INTERNAL` | Construction of the session's engine failed for a non-Kafka reason |

Two refusals are deliberately information-poor: a rejected `Configure`'s Kafka-construction error names
only the exception **class**, never its message - Kafka's configuration exceptions embed property values,
and `kafka_properties` may carry credentials, which must never reach the stream or a log.

Non-fatal discards (unknown token, superseded epoch, malformed report, un-negotiated message) do not close
the stream; the proxy logs and continues. The client-side protocol violation - a `Dispatch` overflowing the
client's queue beyond `max_concurrency` - is failed by the **client** with `FAILED_PRECONDITION` naming the
count.

## Message reference

Conventions: all scalar fields carry explicit presence; "absent" always means the field is not set on the
wire, and is always distinguishable from a zero value. Kafka's null-vs-empty distinction for keys and
values (a tombstone is not an empty value) is preserved as absent-vs-present `bytes`.

### Envelopes

**`ClientMessage`** - everything the client can say. Exactly one of:

| Field | Type | Meaning |
|---|---|---|
| `configure` | `Configure` | First message of a fresh session |
| `report` | `Report` | Per-record outcome |
| `heartbeat` | `Heartbeat` | Connection-level lease extension |
| `manifest` | `Manifest` | First message of a reconnect |
| `worker_died` | `WorkerDied` | Immediate reclaim of a dead worker's records |

**`ProxyMessage`** - everything the proxy can say. Exactly one of:

| Field | Type | Meaning |
|---|---|---|
| `configured` | `Configured` | Handshake reply: the effective configuration |
| `dispatch` | `Dispatch` | A wave of records to process |
| `drop` | `Drop` | Order to discard a superseded delivery |
| `shutdown` | `Shutdown` | Drain order |
| `set_executor_count` | `SetExecutorCount` | Declared unused in v1 - never sent |

### Enums

**`ProcessingOrder`** - mirrors the engine's ordering modes. `PROCESSING_ORDER_UNSPECIFIED` means "take
the proxy's default"; the reply reports what that default was. Values: `PROCESSING_ORDER_UNORDERED`,
`PROCESSING_ORDER_PARTITION`, `PROCESSING_ORDER_KEY` (the default).

**`CommitMode`** - `COMMIT_MODE_UNSPECIFIED` (take the default), `COMMIT_MODE_PERIODIC_CONSUMER_SYNC`,
`COMMIT_MODE_PERIODIC_CONSUMER_ASYNCHRONOUS` (the default), and
`COMMIT_MODE_PERIODIC_TRANSACTIONAL_PRODUCER` - expressible but **always refused** at configure time:
exactly-once through the proxy is impossible in v1, and the enum keeps the value so the refusal can name
exactly what was asked for.

**`InvalidOffsetMetadataPolicy`** - what the engine does on unreadable committed offset metadata:
`INVALID_OFFSET_METADATA_POLICY_UNSPECIFIED` (take the default), `INVALID_OFFSET_METADATA_POLICY_FAIL`
(the default: shut down), `INVALID_OFFSET_METADATA_POLICY_IGNORE` (discard the metadata and continue from
the committed offset).

### `Configure`

The first message of a fresh session; the only place configuration travels. Unset optional fields and
`UNSPECIFIED` enums mean "take the proxy's default" - and `Configured` reports the value the default
resolved to, so a client asserts what it got, not what it asked for. Durations are
`google.protobuf.Duration`; timestamps are `google.protobuf.Timestamp`.

| Field | Type | Meaning |
|---|---|---|
| `topics` | repeated string | Topic-list subscription. Exactly one of `topics` / `topic_pattern` must be given |
| `topic_pattern` | optional string | Regex subscription (Java regex syntax, compiled by the proxy) |
| `max_concurrency` | optional int32 | The in-flight ceiling: max records the proxy has out to the client at once. Must be ≥ 1. Absent = proxy default (16). **There is no "unlimited"** |
| `kafka_properties` | map<string,string> | Kafka client config incl. credentials. Used to build the proxy's consumer and producer; never echoed, never logged, at any level |
| `capabilities` | repeated string | Capability tokens the client implements; empty = v1 baseline |
| `ordering` | optional ProcessingOrder | Processing order (default KEY) |
| `commit_mode` | optional CommitMode | Commit mode (default periodic consumer async) |
| `commit_interval` | Duration | Time between offset commits |
| `default_message_retry_delay` | Duration | Delay before a failed record is retried |
| `send_timeout` | Duration | Ack wait bound for records the proxy produces |
| `offset_commit_timeout` | Duration | Bound on offset commit awaits |
| `shutdown_timeout` | Duration | Bound on non-draining shutdown |
| `drain_timeout` | Duration | Bound on the shutdown drain |
| `threshold_for_time_spend_in_queue_warning` | Duration | Engine warns when a record queues longer than this |
| `sasl_authentication_retry_timeout` | Duration | Window for retrying SASL authentication exceptions |
| `sasl_authentication_exception_retry_backoff` | Duration | Backoff between those retries |
| `max_failure_history` | optional int32 | Failure records kept per record for retry introspection |
| `invalid_offset_metadata_policy` | optional InvalidOffsetMetadataPolicy | See enum |
| `launch_token` | optional string | **Reserved, unused in v1.** Post-v6 admission hardening: a per-launch secret inherited from the parent |
| `terminal_topic` | optional string | Destination for `Terminal` records. **Presence, together with the negotiated `terminal` capability, enables terminal reports** - `Configured.terminal_topic` reports whether it took effect; a `Terminal` without an effective topic is a protocol violation |
| `lease_duration` | Duration | How long records survive without heartbeats before returning to scheduling |
| `heartbeat_interval` | Duration | How often the client must send `Heartbeat` |
| `reconnect_window` | Duration | How long the proxy protects records after connection loss (default 30s) |
| `message_buffer_size` | optional int32 | Fixed engine message buffer size, overriding the dynamic load factor |
| `initial_load_factor` | optional int32 | Dynamic load factor start value |
| `maximum_load_factor` | optional int32 | Dynamic load factor ceiling |
| `pc_instance_tag` | optional string | Instance tag for the engine's metrics/logging |

### `Configured`

The handshake reply: the **effective** values the proxy is running with. Field numbers align with
`Configure` for the same concept; field 19 is reserved (`launch_token` is deliberately never echoed), and
`kafka_properties` has **no counterpart field at all** - the echo cannot leak credentials because there is
nothing to fill.

Every value the proxy computes is **always set** despite the `optional` markers (which exist for
presence-API stability across generated languages, not to permit absence): a `Configured` missing
`max_concurrency` or `executor_count` is a protocol violation, and **absence never means "unlimited"**.
`pc_instance_tag` is set exactly when configured; `topics`/`topic_pattern` echo whichever subscription
form was given.

Four fields are **capability-gated**, and their absence is how the wire says the machinery is off for
this session, not a violation: `lease_duration` and `heartbeat_interval` are set exactly when
`heartbeat` is in the negotiated set, `reconnect_window` exactly when `manifest` is, and
`terminal_topic` exactly when `terminal` is negotiated **and** `Configure` named a topic. So a
`Configure` naming a `terminal_topic` on a session that did not negotiate `terminal` is accepted, but
the echo omits the topic and terminal reports stay violations - the client learns its terminal feature
is off by reading the echo, which is the general rule: assert what came back, never what was asked for.

| Field | Type | Meaning |
|---|---|---|
| `topics` / `topic_pattern` | as Configure | The fixed subscription |
| `max_concurrency` | optional int32 | The effective in-flight ceiling - also the client's dispatch-queue depth |
| `executor_count` | optional int32 | How many executors the client runs. Sent once, never revised |
| `capabilities` | repeated string | The negotiated intersection |
| `ordering`, `commit_mode`, all Durations, `max_failure_history`, `invalid_offset_metadata_policy`, `terminal_topic`, `lease_duration`, `heartbeat_interval`, `reconnect_window`, `message_buffer_size`, `initial_load_factor`, `maximum_load_factor`, `pc_instance_tag` | as Configure | The effective value of each |

### `Token`

The fencing token. Opaque to the client; echoed verbatim.

| Field | Type | Meaning |
|---|---|---|
| `record_id` | optional string | Stable identity of the consumed record. Never parsed by the client |
| `epoch` | optional int64 | The delivery count captured at dispatch. **64-bit**; a parser truncating to 32 bits fails the golden bytes |

### `Record`

One Kafka record, as bytes - the proxy never deserializes; keys and values cross the wire as the bytes
Kafka held, and deserialization happens in the worker's language.

| Field | Type | Meaning |
|---|---|---|
| `topic` | optional string | Source topic |
| `partition` | optional int32 | Source partition |
| `offset` | optional int64 | Source offset |
| `key` | optional bytes | Absent = null key (not empty) |
| `value` | optional bytes | Absent = null value (a tombstone is not an empty value) |

### `Dispatch` and `DispatchRecord`

`Dispatch` is a **wave**: `records` (repeated `DispatchRecord`), one or more records in one protocol
message. Each record is reported independently. Hand-out inside the client is FIFO by this message's record
order.

`DispatchRecord` carries the delivery state an in-process user function would see:

| Field | Type | Meaning |
|---|---|---|
| `token` | `Token` | The fencing token for this delivery |
| `record` | `Record` | The Kafka record |
| `attempt` | optional int32 | 1 on first delivery, 2 on first redelivery. Product data - distinct from the epoch, which also counts verdict-free redeliveries that consumed no attempt |
| `last_failure_at` | optional Timestamp | Absent on first delivery; presence is the wire form of "has failed before" |
| `last_failure_reason` | optional string | The previous failure's worker-supplied text, verbatim. Untrusted input |

### `Report` and its outcomes

| Field | Type | Meaning |
|---|---|---|
| `token` | `Token` | The dispatched token, echoed verbatim |
| `success` | `Report.Success` | Completed; `produce` (repeated `ProduceRecord`) is optional output for the proxy to produce |
| `failure` | `Report.Failure` | Failed; optional `reason` rides the redelivery. Untrusted input |
| `terminal` | `Report.Terminal` | Terminally failed; optional `reason` accompanies the terminal record. Untrusted input |
| `released` | `Report.Released` | Returned unrun (shutdown path); empty message |

### `ProduceRecord`

A record the worker asks the proxy to produce on success.

| Field | Type | Meaning |
|---|---|---|
| `topic` | optional string | Destination topic |
| `key` | optional bytes | Absent = null key |
| `value` | optional bytes | Absent = null value |

### `Heartbeat`

Empty, deliberately: the lease is connection-scoped, so there is nothing per-record to say. Sent every
`heartbeat_interval`, unconditionally.

### `Manifest`

`tokens` (repeated `Token`): the deliveries the client's **live workers** still hold, echoed verbatim.
First message of a reconnect stream. Queued-but-never-executing records were discarded at connection loss
and must not appear.

### `WorkerDied`

`tokens` (repeated `Token`): the deliveries a dead worker held. The proxy returns them to scheduling
immediately, attempt counts unchanged.

### `Drop`

`token` (`Token`): the superseded delivery to discard. The client drops the worker's eventual result and
sends no report for it.

### `Shutdown`

Empty. The drain order; see [Shutdown and drain](#shutdown-and-drain).

### `SetExecutorCount`

`executor_count` (optional int32). **Declared and unused in v1** - see
[The executor count](#the-executor-count).

## A complete session, message by message

The transcript below is the golden session, committed as bytes in the golden resources - with one
deliberate difference: the reconnect's repeated `Configured` is byte-identical to the first and appears
once in the bytes, so the byte files hold eight client messages and five proxy messages (see
[Wire contract at a glance](#wire-contract-at-a-glance) for the exact list and the corpus's
parser-fixture-not-legal-session status). `→` is client to proxy, `←` is proxy to client.

```text
# the application launches the sidecar; the sidecar prints its port
proxy stdout: "port: 50051"

# the client connects to 127.0.0.1:50051 and opens Session; fresh handshake
→ ClientMessage{ configure: {
      topics: ["demo-topic"], max_concurrency: 2,
      kafka_properties: {"bootstrap.servers": "localhost:9092"},
      capabilities: ["dispatch"],            # abbreviated; a real client declares its full set or nothing
      ordering: KEY, commit_mode: PERIODIC_CONSUMER_SYNC,
      commit_interval: 1s, default_message_retry_delay: 1s, send_timeout: 10s,
      offset_commit_timeout: 10s, shutdown_timeout: 10s, drain_timeout: 30s,
      threshold_for_time_spend_in_queue_warning: 10s,
      sasl_authentication_retry_timeout: 0s, sasl_authentication_exception_retry_backoff: 1s,
      max_failure_history: 10, invalid_offset_metadata_policy: FAIL,
      launch_token: "per-launch-token-unused-in-v1",
      terminal_topic: "demo-topic.terminal",
      lease_duration: 60s, heartbeat_interval: 5s, reconnect_window: 30s,
      message_buffer_size: 500, initial_load_factor: 2, maximum_load_factor: 100,
      pc_instance_tag: "golden-session" } }

# the proxy builds its Kafka clients, subscribes, and answers with the EFFECTIVE configuration
← ProxyMessage{ configured: { ...same values..., executor_count: 2, capabilities: ["dispatch"] } }
# the client now spawns 2 executors and creates its dispatch queue, depth 2 (= max_concurrency)

# a wave: two records, distinct shards, one message
← ProxyMessage{ dispatch: { records: [
      { token: {record_id: "demo-topic/0/0", epoch: 1},
        record: {topic: "demo-topic", partition: 0, offset: 0, key: "key-a" /* value ABSENT: tombstone */},
        attempt: 1 },
      { token: {record_id: "demo-topic/0/1", epoch: 5000000000},   # int64: beyond int32, deliberately
        record: {topic: "demo-topic", partition: 0, offset: 1, key: "key-b", value: "hello"},
        attempt: 2, last_failure_at: 2023-11-14T22:13:20.000000001Z,
        last_failure_reason: "worker exploded" } ] } }

# the admin heartbeats on its interval, whatever the executors are doing
→ ClientMessage{ heartbeat: {} }

# per-record outcomes, any order; tokens echoed verbatim
→ ClientMessage{ report: { token: {record_id: "demo-topic/0/0", epoch: 1},
      success: { produce: [{topic: "demo-topic.out", key: "key-a", value: "world"}] } } }
→ ClientMessage{ report: { token: {record_id: "demo-topic/0/1", epoch: 5000000000},
      failure: { reason: "worker exploded" } } }        # returns to retry scheduling
→ ClientMessage{ report: { token: {record_id: "demo-topic/0/1", epoch: 5000000000},
      terminal: { reason: "poison pill" } } }           # (illustrative: on its later redelivery)

# a worker process crashed while holding demo-topic/0/2: reclaim it now, no waiting
→ ClientMessage{ worker_died: { tokens: [{record_id: "demo-topic/0/2", epoch: 2}] } }

# --- connection drops; live workers keep running; the client discards its queued records ---
# reconnect within the window: the new stream opens with the manifest of what live workers hold
→ ClientMessage{ manifest: { tokens: [
      {record_id: "demo-topic/0/0", epoch: 1},          # current: stays in flight
      {record_id: "demo-topic/0/1", epoch: 5000000000} ] } }  # superseded: proxy answers Drop
← ProxyMessage{ configured: { ...unchanged effective configuration... } }
← ProxyMessage{ drop: { token: {record_id: "demo-topic/0/1", epoch: 5000000000} } }

# drain: the proxy orders shutdown; queued records come back Released, executing ones report normally
← ProxyMessage{ shutdown: {} }
→ ClientMessage{ report: { token: {record_id: "demo-topic/0/2", epoch: 2}, released: {} } }
# client half-closes; proxy commits, completes the stream, exits

# never seen in v1 - in the golden corpus only so every parser is proven against every type:
← ProxyMessage{ set_executor_count: { executor_count: 2 } }
```

## Settled at the freeze

Decisions a client author might otherwise wonder about, recorded with their reasons:

- **`Token.epoch` is int64.** Its source is a 64-bit counter; the provisional schema's int32 was widened
  at the freeze, while widening was free - post-freeze it would be a breaking change.
- **`launch_token` exists now, unused.** The post-v6 security work needs a connect-time secret; adding an
  optional field then is additive, but a *required* one would break every client - so the field is
  reserved while reserving costs nothing.
- **`OUTCOME_NOT_SET` is a violation, never a default.** See [Outcomes](#outcomes).
- **`Configured.max_concurrency` absent is a violation, never "unlimited".** The ceiling is always finite
  and always reported; the `optional` marker is presence-API plumbing, not license to omit.
- **Capabilities are the version.** No version number field; see
  [Capabilities](#capabilities-and-versioning).
- **No shutdown-request message.** Client-initiated shutdown is report-everything-then-half-close; a
  message would carry no information the half-close does not.
- **The executor-count formula is unspecified on purpose** - it is an open product question, and the wire
  is already shaped so any answer (including a future dynamic one, via `SetExecutorCount` plus its
  capability) is additive.
