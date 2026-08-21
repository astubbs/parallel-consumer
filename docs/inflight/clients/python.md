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

### The contract's Python rule is aimed at the wrong thing, and this is the one to resolve

The contract says the simulated work "must use that language's non-occupying wait", and singles
Python out because "the client runs worker *processes*; a hundred sleeping processes is not the free
thing a hundred sleeping threads is". The second half is true. The rule that follows from it is not.

`time.sleep` **is** Python's non-occupying wait: it releases the GIL and parks the thread on the
kernel's timer, costing no CPU and no lock. The alternative the rule rules out - a busy loop - is
what would pin a core per in-flight record. What a Python wait occupies is a whole worker *process*,
and **no wait primitive changes that**: this client hands a worker one record and takes one outcome
back, so an event loop inside the worker cannot overlap a second record, and
`asyncio.run(asyncio.sleep(d))` per record would hold the process for exactly as long plus a loop
set-up.

TypeScript's entry in the same list is sound - one event loop, and a blocking sleep there stops
everything, so an awaited timer genuinely changes the behaviour. Python's is not the same shape: the
cost is the process count, so the divergence belongs on `--concurrency`, which is where this demo
put it. **Suggested rewording for whoever owns the next contract pass:** Python's divergence is the
default concurrency, not the wait primitive.

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
`int` would be duplicated by the fork and every worker would count only its own records.
