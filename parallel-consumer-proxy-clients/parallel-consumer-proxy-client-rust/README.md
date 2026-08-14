# Parallel Consumer - Rust proxy client

Key-ordered concurrent Kafka processing from Rust, with the Parallel Consumer engine running as a
sidecar process. The crate speaks the frozen v1 proxy protocol
([`protocol-specification.md`](../../parallel-consumer-proxy/docs/protocol-specification.md),
[`client-authoring-guide.md`](../../parallel-consumer-proxy/docs/client-authoring-guide.md)) and
nothing else - it never reads the proxy's Java.

**Wave one, not for use yet** (astubbs#242). Connect, `Configure`, one `Dispatch` wave, the user's
function, the report, and a clean client-initiated shutdown. Heartbeats and the liveness lease, the
manifest reconnect, worker-death reporting, terminal outcomes and the shutdown drain are not
implemented - and therefore not declared: `Configure.capabilities` carries exactly
`["dispatch"]`, because an empty list would mean "the whole v1 baseline" and invite duties this
client does not perform.

## The surface

```rust
use parallel_consumer_proxy_client::{ClientOptions, InboundRecord, Outcome, ParallelConsumerClient};

let client = ParallelConsumerClient::connect(ClientOptions {
    sidecar_path: "/opt/parallel-consumer/proxy".into(),   // absolute, never PATH-resolved
    topics: vec!["orders".to_owned()],
    ..Default::default()
})
.await?;

client.poll(|record: InboundRecord| async move {
    do_work(&record).await?;          // any std error becomes the failure outcome
    Ok(Outcome::success())
})?;

client.closed().await;                // or get on with other work
client.shutdown().await?;
```

- **`Err` is the failure outcome.** Rust has no exceptions, so there is nothing to translate: the
  processor returns `Result<Outcome, ProcessingError>`, `ProcessingError` converts from any
  `std::error::Error`, and `?` is the whole failure path. A panic is caught and reported as a
  failure too.
- **`poll` returns immediately**, once processing is running; `closed()` awaits the session's end.
  A blocking `poll` would leave `shutdown` reachable only from a second task.
- **Blocking user code goes through `blocking(...)`**, which runs it on the blocking pool so it
  cannot stall the task that reads the session stream.
- **The processor is a trait**, `RecordProcessor`, with a blanket implementation for async
  closures - so a named type works for a caller who wants state, and a closure needs no adapter.
  A closure passed to `poll` needs its parameter annotated (`|record: InboundRecord|`): `poll` is
  generic over `RecordProcessor`, not over `Fn`, and nothing else pins the type.

## Building and testing

```bash
# once, so the conformance harness exists (it lives in the proxy module's TEST jar, and this
# module deliberately has no Maven dependency on the engine, so -am cannot pull it in)
bin/build.sh -pl :parallel-consumer-proxy -am -DskipTests

cd parallel-consumer-proxy-clients/parallel-consumer-proxy-client-rust
cargo clippy --all-targets -- -D warnings     # THE LINT GATE - run it before every commit
cargo test                                    # unit, integration and doc tests
```

`cargo test` spawns the real sidecar over real gRPC, so it needs a JVM: `JAVA_HOME` (or
`PC_PROXY_TEST_JAVA`) pointing at JDK 17. `protoc` is needed to build at all - `build.rs` explains
where it looks and how to override it.

Through Maven, the same two commands are the module's build and test steps, active only under the
opt-in profile:

```bash
bin/build.sh -pl :parallel-consumer-proxy-client-rust -am -Dpc.foreignClients
```

Without `-Dpc.foreignClients` an ordinary build of this module runs no Rust toolchain whatsoever.
Note that cargo and Maven share `target/`, so `mvn clean` deletes the Rust build output too and the
next foreign build recompiles the dependency tree - deliberately, since a clean that spares a
foreign toolchain's output is a clean that lies.

## Layout

| Path | What it is |
|---|---|
| `build.rs` | Generates the protobuf/gRPC bindings from the frozen `.proto`, at build time - so there is no committed generated code to drift, and no regeneration check to remember |
| `src/client.rs` | The session: handshake, transport task, dispatch queue, executors, shutdown |
| `src/options.rs` | `ClientOptions` and the capability tokens; its `Debug` redacts `kafka_properties` |
| `src/outcome.rs` | The processor trait, `Outcome`, and `ProcessingError` - the no-exceptions answer |
| `src/sidecar.rs` | The child process and the lifecycle pipe that reaps it |
| `tests/session.rs` | The one-record conformance scenario, end to end against the sidecar |
| `tests/harness/mod.rs` | Locating and spawning the JVM-side test-mode sidecar |

Findings, divergences and what wave two owes are in
[`docs/inflight/clients/rust.md`](../../docs/inflight/clients/rust.md).
