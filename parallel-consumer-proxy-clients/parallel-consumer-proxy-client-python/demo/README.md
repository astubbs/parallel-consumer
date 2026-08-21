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
sibling. It announces which it chose, and why, on its first line.

**The contract this keeps - and that every other language's demo keeps - is
[`parallel-consumer-proxy/demo/README.md`](../../../parallel-consumer-proxy/demo/README.md).**
Read that first. This file only records what is specific to Python.

## The two arms

| arm | what it is |
|---|---|
| **AK core** | `confluent_kafka.Consumer` - Python's own Apache Kafka client - one record at a time, in this process. |
| **python-grpc** | This module's client library. It spawns the sidecar as a child process, receives records over a socket, runs the user's function in **worker processes**, and reports outcomes back. The application does no Kafka I/O on this path. |

Two arms is the whole contract outside Java. The seed carries four more because one JVM can hold
every engine at once; Python has one Kafka client and one way to reach the engine, so a wrapper arm
and a raw-wire arm would have nothing to be compared against. In particular **nothing here speaks
the protocol by hand** - the seed's first version did, and it proved the engine worked while saying
nothing about the client library, which is the artifact users actually touch.

## What is specific to Python

### `--concurrency` defaults to 16, not the seed's 100

**This is the only default that differs, and it is not a preference.** The proxy's executor-count
function is `IntUnaryOperator.identity()` today, so a client asking for 100 in-flight records gets
**100 executors** - and in Python an executor is a worker *process*, not a thread. A hundred
interpreters on a laptop measures the machine's memory pressure rather than the engine.

Sixteen is a number a laptop can hold. It is fixed rather than derived from `os.cpu_count()`, so
that two readers' fingerprints are comparable. `--concurrency 100` still does exactly what it says.

The formula itself is an open owner decision, not this demo's to make -
[`docs/inflight/blocker-executor-count-formula.md`](../../../docs/inflight/blocker-executor-count-formula.md).

### The simulated work is `time.sleep`, and the divergence is smaller than the contract expects

The contract asks Python for a "non-occupying wait", on the grounds that "a hundred sleeping
processes is not the free thing a hundred sleeping threads is". That last part is true; the
conclusion does not follow to the *wait primitive*, and this demo diverges on the default above
instead.

`time.sleep` releases the GIL and parks the thread on the kernel's timer: the wait costs no CPU and
no lock, which is exactly what the rule is protecting against - a busy loop would pin a core per
in-flight record. What a Python wait occupies is a whole worker **process**, and no wait primitive
changes that: this client hands a worker one record and takes one outcome back, so an event loop
inside the worker cannot overlap a second record. `asyncio.run(asyncio.sleep(d))` per record would
hold the process for exactly as long, plus a loop set-up. TypeScript's divergence is real - one
event loop, and a blocking sleep there stops everything - Python's is not the same shape.

`simulate_work` in [`reference_demo.py`](reference_demo.py) carries the same reasoning at the point
of use, and [`docs/inflight/clients/python.md`](../../../docs/inflight/clients/python.md) records
it as a contract wording this demo believes is wrong.

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

## The files

| file | what it is |
|---|---|
| [`run.sh`](run.sh) | The entry point. Same flags as the seed, plus `--docker` / `--native`. |
| [`reference_demo.py`](reference_demo.py) | The arms, the timing and the two tables. |
| [`demo_options.py`](demo_options.py) | The flags, the environment variables and the precedence between them. |
| [`demo_kafka.py`](demo_kafka.py) | The topic, the seeded backlog, and the properties both arms' consumers use. |
| [`Dockerfile`](Dockerfile) | A JDK base with a Python interpreter added - two toolchains, because the application is Python and the sidecar is a JVM. |
| [`docker-compose.yml`](docker-compose.yml) | The demo and its broker sibling. Also the broker the native path uses. |

`make demo-build` is what installs the demo's one extra dependency, `confluent-kafka`. It is
deliberately not in the `dev` extra: the library never imports it, and neither the test suite nor
the conformance runner needs it.
