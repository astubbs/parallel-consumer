# Python client (astubbs#242)

Wave one landed: connect, configure, one dispatched record through a worker process, report, clean
drain. **The demo landed on `demos/python`** (`parallel-consumer-proxy-client-python/demo/`) - two
arms, both entry points, container included. Leases, heartbeats, reconnect, worker death, terminal
outcomes and PyPI packaging are all still deferred and named in the module's testing-evidence
`limitation`.

## The demo's divergences from the shared contract, and which of them is a contract defect

The contract is `parallel-consumer-proxy/demo/README.md`, which the demo wave may not edit. These
are recorded here for the integrator to resolve.

### `--concurrency` defaults to 16, not the seed's 100 - forced, not chosen

The proxy's executor-count function is `IntUnaryOperator.identity()`
(`docs/inflight/blocker-executor-count-formula.md`), so `max_concurrency` **is** the worker count,
and in Python a worker is a process. The seed's default of 100 means a hundred interpreters. Sixteen
is what the demo asks for instead; `--concurrency` still does exactly what it says, and the
fingerprint prints the effective value, so nothing is hidden.

Fixed rather than derived from `os.cpu_count()` on purpose: a default that changes with the machine
cannot be compared between two readers' runs, and the fingerprint would be the only place the
difference showed.

**This does not unblock the blocker.** It is a demo picking a survivable number; the formula is
still an open owner decision that every one of the ten client authors inherits.

### The contract's two new columns stop `bin/ci-demo-conformance.sh` seeing any table at all

**Measured, not predicted**: the six-column table this demo now prints, fed through that script's
own `skeleton()` awk, produces **no `HEADER` line and no `ROW` lines** - only the `DIAL` and `TITLE`
ones. Two independent causes, and either alone is enough:

- the header pattern requires `arm` to be followed immediately by `elapsed`, and `records` and
  `keys` now sit between them;
- the row pattern allows `[A-Za-z0-9 _-]` in an arm name, and every arm label now carries its
  client's name in **brackets** - `AK core (confluent-kafka)`.

The script still exits 0. Its absolute assertions read `DIAL` and `TITLE` lines, which survive, so
the run stays green while the drift check silently narrows to *the fingerprint and the two
headings* - it can no longer see a table's columns, its arms, or their order, which was most of
what it was for. That is the check-that-passes-without-having-run class again
(`docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md`).

`bin/**` is outside the language waves' file scope, so **the integrator owns the repair**, and it
is worth more than a regex fix: `records` and `keys` are *deterministic*, which the contract says
is exactly what the harness relies on - so the skeleton should carry their **values** rather than
masking them like elapsed and msg/s. Today it masks every figure, so eleven languages could each
report a different record count and the drift check would still be clean.

### `KAFKA_LOG4J_ROOT_LOGLEVEL: WARN` does not quieten the broker, in any of the eleven files

The contract's "the broker is quiet" clause is not being met, and the compose setting that was
supposed to meet it is not enough. Observed on this demo's container path against
`confluentinc/cp-kafka:7.9.0`: the env var is set on the container, the generated config does say
`log4j.rootLogger=WARN, stdout` - and the broker still printed **927 lines** of controller
elections and log-segment chatter, interleaved by compose with the **35** the demo itself produced,
which is exactly the burial the clause was written to prevent. The cause is
that the image also writes **per-logger levels that override the root**:

```
log4j.logger.kafka=INFO
log4j.logger.kafka.controller=TRACE
log4j.logger.state.change.logger=TRACE
log4j.logger.kafka.log.LogCleaner=INFO
```

Those come from the image's default `KAFKA_LOG4J_LOGGERS`, so the fix is to set that variable too
rather than to change the root level - roughly
`KAFKA_LOG4J_LOGGERS: "kafka=WARN,kafka.controller=WARN,state.change.logger=WARN,kafka.log.LogCleaner=WARN"`.

**Not applied here on purpose.** It is one line in eleven identical compose files and the demo wave
was told to leave broker log levels alone; fixing it in one language would leave ten quiet in the
compose file and loud on screen. It belongs in the same pass that set the root level.

### Where the two new columns go is a coordination question the contract does not answer

The contract names `records` and `keys` and says the tables keep "same columns, same order"; it
does not say where in the row they sit. This demo prints
`arm | records | keys | elapsed | msg/s | vs AK core` - what ran, what it did, then how fast, so a
reader meets the evidence before the number it justifies.

Eleven language agents chose independently and simultaneously. Until the harness above can see a
header again, **nothing will detect a disagreement**, so this is a merge-time reconciliation rather
than something CI will catch.

### The banner has to be printed by `run.sh` too, or it is not the first thing a reader sees

`reference_demo.py` prints it, which covers `docker compose up` and running the module by hand. It
is not sufficient on its own: a native run builds the sidecar's classpath with Maven and starts a
broker first, so the product's name would arrive a minute or so into a screen of build output.
`run.sh` therefore prints the same banner before its own first line and sets
`PC_DEMO_BANNER_PRINTED=1` - a statement of fact, not a dial - which the demo honours and the
compose file forwards, so no path prints it twice.

Any language whose `run.sh` builds or installs anything before starting the demo has the same
problem, and a language that put the banner only in its demo program will not have noticed it.

**One consequence to know before writing a check for it**: on the container path launched through
`run.sh`, the banner is on the host's stdout and *not* among the `demo-1 |` lines a compose capture
scopes to. A conformance assertion that looks for the banner in the demo's own output would fail
this language and pass the ones that print it twice, which is the wrong way round. Assert it on the
whole capture, or settle the wrapper-versus-demo question first.

### Both arms are timed from the first record, not from before consumption

The seed's clock starts just before consumption, with client construction and the sidecar spawn
already outside the window - "no other arm charges itself for client construction or teardown".
`ParallelConsumerClient.poll()` forks the worker pool, spawns the sidecar, completes the handshake
and starts consumption in **one call**, so that line cannot be drawn in the same place here.

The demo keeps the seed's rule instead of its line number: start-up is outside the window for both
arms, at the first record either sees. Measured, on the run that settled it: including the JVM boot
would have reported roughly a quarter of the sidecar arm's rate. Any language whose client library
bundles spawn and start into one call - Ruby and C++ next - inherits this question.

### The demo never starts a broker; `run.sh` does

The seed uses Testcontainers. The Python equivalent would put a Docker client library into the demo
of a Kafka client library, so `run.sh` brings up the same compose broker its container path uses and
hands the demo an address. The reader-facing promise is unchanged (omit `--bootstrap`, a broker
appears) and the non-negotiable rule is untouched: the demo container is never given the host Docker
socket. `reference_demo.py` invoked directly with no bootstrap says so and exits 2.

Same question for every non-JVM language, and the compose broker is the cheaper answer in all of
them.

## Not done, and owed to whoever picks this up

- **`bin/ci-demo-test.sh` runs the Java demo only** - the script hard-codes the Java module's paths
  and arm names, and it is outside this wave's file scope. Until an integrator generalises it, the
  contract's "both entry points are tested" clause is met by the Java demo alone and the Python
  demo's two entry points are untested in CI. This is the demo defect class the script's own header
  is about, so it matters more than it looks.
- **The native path needs a JDK**, because the sidecar is a JVM binary. `run.sh` builds
  `parallel-consumer-proxy`'s classpath and exports `PC_DEMO_SIDECAR_CLASSPATH`. The demo already
  reads `PC_DEMO_SIDECAR` for an absolute binary, so the day a native sidecar exists the JDK half
  drops out of `run.sh` and out of the Dockerfile with no other change.
- **`dependency:build-classpath` needs `-DincludeScope=runtime`**, and the failure is quiet: the
  default scope pulls in `parallel-consumer-core`'s test jar, whose `logback-test.xml` configures
  the sidecar's logging and prints logback's own status report to **stdout**, ahead of the
  `port: <n>` line the client library scans for. It survives only because that scan tolerates
  preceding lines. Any language whose demo spawns the JVM sidecar from a Maven-built classpath hits
  this.
- **`src/docs/development/upstream-map.yaml` still has no entry for this work** - outside the wave's
  file scope, and unchanged from wave one.

## Two spec divergences, both confirmed independently by the Go wave

These are **specification defects, not Python's local choices** — the Go client hit both without
seeing the Python work, which is why they are recorded here rather than in a module README.

- **A client cannot answer a protocol violation with a status code.** The authoring guide says to
  fail the stream with `FAILED_PRECONDITION` naming the count when the dispatch queue overflows.
  Only the server side of a gRPC call sets a status; a client can cancel, nothing more. Python
  cancels the call and raises a `ProtocolViolation` naming the count; Go treats it the same way.
  The guide's rule needs rewording for whoever owns the next doc pass — it is unimplementable as
  written, in every language.
- **`Released` on shutdown contradicts capability negotiation.** The guide's shutdown section makes
  the drain unconditional ("`Released` for the queue"), while its negotiation section forbids
  sending any message outside the negotiated set — and the test-mode harness negotiates only
  `["dispatch"]`, so there is no legal action for queued records. Python sends `Released` only when
  `shutdown` is negotiated and otherwise discards the queue for the proxy to reclaim, reasoning that
  sending outside the set would be the client's own violation. That choice is defensible but it is
  the client picking a winner between two rules; the specification should pick.

## The fork ordering, which the plan states as two requirements that cannot both hold

The plan asks for the worker pool to exist before any channel does, *and* to be sized by the count
`Configured` supplies — but that count only arrives after a handshake on an open channel. Resolved
with a **launcher process** forked from a channel-free, thread-free image, which forks the workers
once the count arrives; the application process never forks again after a channel exists. Ordering
inside `poll()` is pool → sidecar → channel, and the sidecar is deliberately second: its
stdout/stderr drain threads would otherwise be inherited by the fork, which Python 3.13 flags.

Worth reading before the same question is answered differently in another language with real
processes (Ruby, C++).

The demo inherits it as a constraint on its own bookkeeping: the counters both replays wait on are
`multiprocessing` primitives created before the client exists, because a closure over an ordinary
`int` would be duplicated by the fork and every worker would count only its own records. The
contract's **unique keys** column lands on the same constraint - a `set` cannot cross a fork either
- and `KeyTally` in `demo/reference_demo.py` answers it with a shared byte per key slot rather than
with a `Manager` dictionary, which would put an IPC round trip on the timed path.
