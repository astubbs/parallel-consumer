<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# The Python demo

```bash
# from anywhere in the repo - picks native or container for you
parallel-consumer-proxy-clients/parallel-consumer-proxy-client-python/demo/run.sh

# or, from this directory, the plain container path with nothing else needed
docker compose up
```

Needs Docker. Python 3.10+ and a JDK are optional: with both, the demo runs natively and starts its
broker in a container; without either, the demo runs in a container too and the broker is a compose
sibling. It prints the product's banner first, then announces which it chose and why.

**The contract this keeps - and that every other language's demo keeps - is
[`parallel-consumer-proxy/demo/README.md`](../../../parallel-consumer-proxy/demo/README.md).**
Read that first. This file only records what is specific to Python.

## The two arms

| arm | what it is |
|---|---|
| **AK core (confluent-kafka)** | `confluent_kafka.Consumer`, one record at a time, in this process. |
| **pc-python-grpc (this client)** | This module's client library. It spawns the sidecar as a child process, receives records over a socket, runs the user's function in **worker processes**, and reports outcomes back. The application does no Kafka I/O on this path. |

Both labels name the library that actually ran, which is contract: **"AK core" is a category, not a
client**, and a reader cannot judge a comparison without knowing what produced it.

Two arms is the whole contract outside Java. The seed carries four more because one JVM can hold
every engine at once; Python has one way to reach the engine, so a wrapper arm and a raw-wire arm
would have nothing to be compared against. In particular **nothing here speaks
the protocol by hand** - the seed's first version did, and it proved the engine worked while saying
nothing about the client library, which is the artifact users actually touch.

## What is specific to Python

### Python has more than one serious Kafka client, and this demo runs `confluent-kafka`

The contract asks a language with several to say so here, because "the choice materially changes
the number, and a reader evaluating *is this fast in my language* is really asking about the client
they already use". Python's three:

| client | what it is |
|---|---|
| [`confluent-kafka`](https://github.com/confluentinc/confluent-kafka-python) | A binding to **librdkafka**, the C client. What this demo's AK core arm runs, and what seeds the backlog. |
| [`kafka-python`](https://github.com/dpkp/kafka-python) | A pure-Python implementation of the protocol - no C library underneath it. |
| [`aiokafka`](https://github.com/aio-libs/aiokafka) | `asyncio`-native, built on `kafka-python`'s protocol code. |

Only `confluent-kafka` runs here. **Nothing in this repository has measured the other two**, so
this is a choice on grounds rather than on numbers: a binding to a C client is the denominator
least likely to flatter the sidecar arm, which makes it the conservative one to divide by. It is
also the client the sidecar itself effectively competes with, since the sidecar's consumer is the
Java client rather than a Python one.

**Running a second AK core arm is a live option** the contract explicitly allows, and this demo has
not taken it. `aiokafka` would be the interesting one - it is the shape an asynchronous Python
application already has - and it would need the demo's serial arm to grow an event loop, which is
why it is recorded as an option rather than done.

### `--concurrency` defaults to 16, not the seed's 100

**This is the only default that differs, and it is not a preference.** The proxy's executor-count
function is `IntUnaryOperator.identity()` today, so a client asking for 100 in-flight records gets
**100 executors** - and in Python an executor is a worker *process*, not a thread. A hundred
interpreters on a laptop measures the machine's memory pressure rather than the engine.

Sixteen is a number a laptop can hold. It is fixed rather than derived from `os.cpu_count()`, so
that two readers' fingerprints are comparable. `--concurrency 100` still does exactly what it says.

The formula itself is an open owner decision, not this demo's to make -
[`docs/inflight/blocker-executor-count-formula.md`](../../../docs/inflight/blocker-executor-count-formula.md).

### The simulated work is `time.sleep`, and that satisfies the contract's predicate

The contract's rule is now a property of the **client**, not of the language: *is it
thread-per-record?* This one is not - it hands the user's function to a worker **process** - so
Python is one of the six that needs its own non-occupying wait.

`time.sleep` **is** that wait. It releases the GIL and parks the thread on the kernel's timer, so
it costs no CPU and no lock; the thing the rule exists to rule out is a busy loop
(`while time.monotonic() < deadline: pass`), which would pin a core per in-flight record. No other
primitive is available that would occupy less: this client hands a worker one record and takes one
outcome back, so an event loop inside the worker cannot overlap a second record, and
`asyncio.run(asyncio.sleep(d))` per record would hold the process for exactly as long plus a loop
set-up.

**What a Python wait occupies is a whole worker process** - and here that is not a
misreport, which is the distinction the contract's predicate is drawing. The rule's hazard is a
table "reporting the runtime's ceiling while appearing to report the engine's". Python's ceiling
*is* the concurrency the fingerprint printed, because the proxy's executor count is the worker
count, so a sleeping worker per in-flight record is exactly the number on the page. The cost lands
on how expensive that number is to buy - which is why this demo's divergence is the default
`--concurrency` above, and not the wait primitive.

`simulate_work` in [`reference_demo.py`](reference_demo.py) carries the same reasoning at the point
of use. An earlier version of the contract named languages rather than stating this predicate, and
listed Python for a reason that did not survive; that argument is settled and no longer tracked in
[`docs/inflight/clients/python.md`](../../../docs/inflight/clients/python.md).

### The `keys` column is counted in shared memory, because a `set` cannot cross a fork

Every language's table reports **records processed** and **unique keys seen**, and both are
contract because they are deterministic - the same records give the same two figures in any
language, which is what makes them comparable when elapsed and msg/s never are.

In Python the second one is awkward for the same reason everything else here is: the user's
function runs in a worker process, so a `set` closed over by the processor would be duplicated by
the fork and each worker would report only what it saw. `KeyTally` in
[`reference_demo.py`](reference_demo.py) uses a shared **byte per key slot** instead - one
unsynchronised write, since the only value ever written is 1 - and both arms use it, so the two
rows are counted the same way. A `multiprocessing.Manager` dictionary would have counted exactly
and for any key at all; it was rejected because it puts an IPC round trip on the critical path of
the arm the demo exists to time.

The price is that a key this demo did not seed has no slot. That only arises if `--topic` names a
topic already holding somebody else's records, and it is never silent: those records are counted
separately and the arm prints that its keys figure is an undercount.

### The clock starts at the first record, in both arms

The seed starts its clock just before consumption, having already built its client and spawned its
sidecar outside the window - "no other arm charges itself for client construction or teardown".
Python cannot draw the line in the same place: `ParallelConsumerClient.poll()` forks the worker
pool, spawns the sidecar, completes the handshake and starts consumption in one call.

So this demo keeps the seed's **rule** rather than its line number: start-up is outside the window
for both arms, at the first record either of them sees. Including a JVM boot in a throughput figure
would report roughly a quarter of the arm's real rate.

### The demo never starts a broker itself

The seed uses Testcontainers. The equivalent here would put a Docker client library into the demo of
a Kafka client library, so `run.sh` brings up the same compose broker its container path uses and
hands the demo an address. From the reader's side the contract's promise is unchanged - omit
`--bootstrap` and a broker appears - and the rule that matters is untouched: **the demo container is
never granted the host Docker socket**, so inside a container the address is a compose sibling
either way.

`reference_demo.py` run by hand with no `--bootstrap` says so and exits 2 rather than guessing.

For the native path the broker publishes a host listener on `127.0.0.1:19095`
(`PC_DEMO_BROKER_PORT` moves it, which matters if you have another language's demo up). `run.sh`
stops the broker on the way out; `PC_DEMO_KEEP_BROKER=1` leaves it running, which is worth about ten
seconds a run while developing.

### The sidecar is a JVM, so the native path needs a JDK

The client library wants an absolute path to a binary. Until a native sidecar exists that binary is
`java`, and the classpath is an argument about the *binary* rather than configuration - bootstrap
servers, credentials, ordering and concurrency still travel only in the connect-time handshake
(R39). `run.sh` builds the classpath and exports `PC_DEMO_SIDECAR_CLASSPATH`; the container bakes it
in at image build time. `PC_DEMO_SIDECAR` names a binary directly, and is the shape this takes the
day a native sidecar lands.

The sidecar is **not a compose service**, deliberately: the client library spawns and supervises it,
so the user never installs, deploys or operates a process (KTD41). A compose service would show a
deployment the product does not ask for.

## A separate demo: Kafka Streams, described from Python

Not a third row in the table above - a different proof, with its own engine module and its
own protocol. It is run by the same script and shares the same broker and seeding.

```bash
# a topology built from Python, running in a JVM Streams engine
demo/run.sh --streams --native
```

**Experimental, and opt-in for that reason.** It speaks a different protocol from the one above -
`streams.proto`, versioned `v1alpha1` - which may change or disappear. The comparison demo's wire is
frozen; this one is not, and they are kept apart so that stays true.

What it does: Python names a source topic, a value transform, a group-by-key and a count, and the
engine assembles a real `StreamsBuilder` from those calls. When a record reaches the transform, the
engine calls **back into Python** and blocks its stream thread until Python answers. The topology
runs in the JVM; the per-record function never leaves this process.

Nothing of the user's code crosses the boundary. `register()` files the callable in a local dict and
sends only an integer token; the engine names the token when it wants an answer. That is what makes
the same design work over a socket today and over a C ABI later - neither side needs the other's
address space.

### What it proves, and what it does not

It proves that a language with **no Kafka Streams implementation of its own** can describe and run
one. That is the whole claim.

It makes no speed claim, and the figures it prints are deliberately framed as within-session: they
exist to show where the time goes, not to compare against anything. On the machine this was written
on, a round trip is a few hundred microseconds and the Python function itself is a rounding error
inside it - which is the useful finding, because it says the boundary is the cost, not the language.

### Reading the counts

The counts are **checked**, not displayed: the demo knows exactly what it seeded, so it compares.

A count is a KTable changelog, so the sink topic carries every intermediate value per key - a key
whose final count is 12 also carries 1 through 11 ahead of it. The demo reads the sink
**last-value-per-key**. Summing the topic instead would report 78 for that key and call a correct
run broken.

It also watches the engine's consumer group *throughout* the run rather than once at the end,
because a rebalance is a transient: a single-member group that gets evicted rejoins within a second
and reads `STABLE` again. Sampling only at the finish would pronounce a run rebalance-free and say
exactly the same thing about a run that had rebalanced twice in the middle. Samples taken while the
engine is still joining are excluded - that is the engine arriving, not a rebalance.

### The failure arm

```bash
# make the Python transform slow, and watch what breaks
demo/run.sh --streams --native --records 300 --function-delay-ms 300 --timeout 45
```

This arm exists to show that a slow foreign function fails **visibly**. It was written expecting the
engine to blow through `max.poll.interval.ms` and be evicted from its group. It is not what happens:
Kafka Streams interleaves its polling and keeps its membership, and the real symptom is that
throughput collapses to a few records a second and the run does not finish. The arm reports what it
actually observed rather than the prediction.

The one thing it never tolerates is a **wrong** count. Missing counts mean the run did not finish;
a wrong count would mean something counted incorrectly, and that fails loudly in either arm.

### There is no container path for this arm

`--streams --docker` is refused rather than ignored, because the demo image's entrypoint is the
comparison demo - a container run would build, start and report the wrong demo while looking like it
had honoured the flag.

## The files

| file | what it is |
|---|---|
| [`run.sh`](run.sh) | The entry point. Same flags as the seed, plus `--docker` / `--native` and `--streams`. |
| [`reference_demo.py`](reference_demo.py) | The arms, the timing and the two tables. |
| [`demo_options.py`](demo_options.py) | The flags, the environment variables and the precedence between them. |
| [`demo_kafka.py`](demo_kafka.py) | The topic, the seeded backlog, and the properties both arms' consumers use. |
| [`demo_jvm.py`](demo_jvm.py) | Finding the JVM. Shared, so the two demos cannot disagree about which one they ran on. |
| [`streams_demo.py`](streams_demo.py) | The Kafka Streams arm: the topology, the Python transform, and the count check. |
| [`Dockerfile`](Dockerfile) | A JDK base with a Python interpreter added - two toolchains, because the application is Python and the sidecar is a JVM. |
| [`docker-compose.yml`](docker-compose.yml) | The demo and its broker sibling. Also the broker the native path uses. |

`make demo-build` is what installs the demo's one extra dependency, `confluent-kafka`. It is
deliberately not in the `dev` extra: the library never imports it, and neither the test suite nor
the conformance runner needs it.
