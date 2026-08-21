# The Rust demo

```bash
# from anywhere in the repo - picks native or container for you
parallel-consumer-proxy-clients/parallel-consumer-proxy-client-rust/demo/run.sh

# or, from this directory, the plain container path with nothing else needed
docker compose up
```

Needs Docker. A Rust toolchain and a JDK are optional: with both, the demo runs natively and starts
its broker in a container; without them, the demo runs in a container too and the broker is a
compose sibling. It announces which it chose, and why, on its first line.

**The contract this keeps - and that every other language's demo keeps - is
[`parallel-consumer-proxy/demo/README.md`](../../../parallel-consumer-proxy/demo/README.md).**
Read that first. This file only records what is specific to Rust.

## The two arms

| arm | what it is |
|---|---|
| `AK core` | `rdkafka`, Rust's own Kafka client, one record at a time on one thread |
| `rust-grpc` | this application as a **foreign client**: the Rust client library spawns the sidecar, receives records over a socket, runs the user's function and reports outcomes back |

Two arms is the whole contract outside Java. The reference demo carries four more because one JVM
can hold an in-process engine, a client library over that engine, and a hand-written protocol
client all at once, so each *pair* changes exactly one term. Rust has none of those comparators:
there is one engine, reachable one way.

**On the `rust-grpc` path this application does no Kafka I/O.** The sidecar owns the consumer, the
producer, the group membership and the offsets. That is a claim about the *path*, not about the
process - the same binary creates the topic, seeds the backlog and runs the AK core arm with
`rdkafka`, because a comparison needs both sides.

## What is specific to Rust

### The simulated work is a blocking sleep, and *where* it blocks is the whole story

The contract says a blocking sleep is fine in Rust, unlike Python (worker processes) and TypeScript
(one event loop). It is - and Rust still has an answer of its own, because the client library's
executors are tasks on an async runtime.

The demo's user function is `std::thread::sleep`, handed to the library through
[`blocking(...)`](../src/outcome.rs), which is its documented entry point for a blocking user
function: each invocation runs on the runtime's blocking pool. Blocking *directly* inside an
executor task instead occupies a runtime worker thread, of which there are as many as the machine
has cores - so concurrency silently falls from the engine's ceiling to the core count.

Measured rather than reasoned about, one term changed and everything else identical (12 cores,
40,000 records, `--delay-ms 2`, `--concurrency 100`):

| user function | msg/s |
|---|---|
| `blocking(\|record\| thread::sleep(..))` | 10,341 |
| `async move { thread::sleep(..) }` | 3,518 |

The second figure is roughly the core count divided by the delay, which is the predicted ceiling
for a runtime whose workers are all asleep. **Both are honest blocking sleeps**; only one of them
lets the engine's ceiling mean anything. Every async-runtime language should be asked the same
question - the note in `docs/inflight/clients/rust.md` carries it to the wave sync.

### The sidecar is a JVM, and the demo says so out loud

The proxy ships as a Java program, so "the sidecar binary" is the JVM launcher and the proxy's
classpath is an argument to it. A Rust *application* never learns that - it hands the client library
an absolute path and a list of arguments - but this demo builds the sidecar from source, so
`run.sh` builds the proxy module first and leaves its classpath where `src/sidecar.rs` looks for
it. That is why the container carries a JDK as well as a Rust toolchain.

### The native broker comes from this directory's compose file, not from Testcontainers

The reference demo starts its native broker from inside the JVM, with Testcontainers. Rust's
equivalent would put a Docker API client in the demo binary's dependency tree for the one path that
already has Docker, so the demo runs `docker compose up --detach --wait broker` against the compose
file beside this README and reaches it over a **host listener** the reference's broker does not
need. One broker definition serves both paths, so they cannot drift into measuring different
brokers.

Two consequences worth knowing:

- **The broker outlives the run.** Testcontainers reaps its container; a compose service is meant to
  persist. The demo prints the `docker compose ... down` command on the way out rather than
  pretending to own a teardown it does not.
- **In a container the demo refuses to start a broker at all**, rather than reaching for the host
  Docker socket. It is handed `PC_DEMO_BOOTSTRAP` by compose and says so if it is not.

### The sidecar's own logs are invisible from Rust

The proxy carries no logback configuration, so logback's default console appender sends its log
lines to **stdout** - and stdout is the lifecycle channel the client library drains and discards
after reading the port line. `sidecar_stderr` governs the other stream, so it does not bring them
back: the demo's runs show none of the sidecar's several dozen start-up lines. Genuinely fatal JVM
output still reaches stderr and is inherited. That is a property of the client library and the
proxy together, not of this demo; it is recorded in `docs/inflight/clients/rust.md`.

## Cosmetics that differ, and deliberately do not matter

The reference demo prints through slf4j, so its lines carry timestamps and levels. This one uses
plain `println!`. The tables, their columns, their order, the fingerprint and the arm names are
identical, because those are what the contract governs.
