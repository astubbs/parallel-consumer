<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Parallel Consumer - Rust proxy client

> **⚠️ EXPERIMENTAL - not for production use.** Everything in this module is new, unreleased and
> unproven: nothing is published to any package registry, the API may change without notice, and
> the v1 proxy protocol is frozen but has never carried production traffic. Build it from this
> checkout, read it, test it - do not depend on it. Tracking: astubbs#242.

Key-ordered concurrent Kafka processing from Rust, with the Parallel Consumer engine running as a
sidecar process. The crate speaks the frozen v1 proxy protocol
([`protocol-specification.md`](../../parallel-consumer-proxy/docs/protocol-specification.md),
[`client-authoring-guide.md`](../../parallel-consumer-proxy/docs/client-authoring-guide.md)) and
nothing else - it never reads the proxy's Java.

**Wave one.** Connect, `Configure`, dispatch waves, the user's function, per-record reports,
records produced back on success, and a clean client-initiated shutdown. Heartbeats and the liveness
lease, the manifest reconnect, worker-death reporting, terminal outcomes and the shutdown drain are
not implemented - and therefore **not declared**: `Configure.capabilities` carries exactly
`["dispatch"]`, because an empty list would mean "the whole v1 baseline" and invite duties this
client does not perform. They are un-negotiated capabilities, not half-built features.

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
cd parallel-consumer-proxy-clients/parallel-consumer-proxy-client-rust
cargo clippy --all-targets -- -D warnings     # THE LINT GATE - run it before every commit
cargo test                                    # unit, integration and doc tests
```

`cargo test` spawns the real sidecar over real gRPC, so it needs a JVM: `JAVA_HOME` (or
`PC_PROXY_TEST_JAVA`) pointing at JDK 17. `protoc` is needed to build at all - `build.rs` explains
where it looks and how to override it.

Through Maven those same two commands *are* the module's build and test steps:

```bash
./mvnw compile -Dpc.foreignClients -pl :parallel-consumer-proxy-client-rust -am   # the clippy gate
./mvnw package -Dpc.foreignClients -pl :parallel-consumer-proxy-client-rust -am   # cargo test, and the CI row
```

This module is `packaging: pom` with four `pc.foreign.*` properties naming the cargo commands, and
the `foreign-clients` profile in the clients aggregator ([`../pom.xml`](../pom.xml)) binds them to
`compile` and `test` and decides whether the module is in the reactor at all. Without it, an
ordinary build of this module runs no Rust toolchain whatsoever.

- **`compile` is the lint, not the build.** `cargo clippy` type-checks every target and `-D
  warnings` turns any finding into a failure, so `mvn compile` here is stricter than javac would be:
  an unused variable fails it. The real compile happens at `test`. The pom carries why.
- **`-am` is not optional for `compile` or `test`.** `-pl` alone fails the enforcer's
  `ReactorModuleConvergence` with a message about parent modules, which reads as a broken pom;
  [`docs/inflight/bug-scoping-a-build-to-one-client-module-fails.md`](../../docs/inflight/bug-scoping-a-build-to-one-client-module-fails.md)
  owns that. `./mvnw clean -P foreign-clients -pl :parallel-consumer-proxy-client-rust` still needs
  the profile - without it the module is not in the reactor at all - but needs no `-am`, the clean
  lifecycle never reaching `validate` where the enforcer is bound.
- **Reaching the test phase needs `-Dpc.foreignClients`, not `-P foreign-clients`.** Both activate
  the module, but the `rust-e2e-harness` profile - which pulls the proxy module into the reactor for
  the sidecar the test spawns - activates on the *property*. `-am` then builds the engine, so the
  older instruction to build the proxy by hand first is no longer a prerequisite. The flip side is
  worth knowing: `-P` leaves the engine out of the reactor - three modules instead of six, and no
  JDK 17 needed - which makes it the quicker loop when all you want is the clippy gate.
- **`package`, not `test`, for that lane**, because `tests/harness` looks for the proxy's test jar as
  a *file* and `test` stops one phase short of producing one. Same reason the CI row runs `package`.

### What a Java engineer will find surprising here

- **Cargo and Maven share `target/`**, this module having no workspace and no `build.target-dir`. So
  `mvn clean` deletes the Rust build output as well, and the next foreign build recompiles the
  dependency tree from nothing. That is deliberate - a clean that spares a foreign toolchain's
  output is a clean that lies - and it is why the pom configures no `maven-clean-plugin` fileset:
  the default one already covers it. Verified both ways here: `target/debug` present after a build,
  gone after `mvn clean`, and the next build green.
- **The fetched crates are not output and do not go.** The registry and its sources live in
  `~/.cargo`, which is this language's `~/.m2`; `mvn clean` does not empty `~/.m2`. `cargo clean` is
  not used either - it needs the toolchain present to delete files, and the whole premise of the
  opt-in profile is that the toolchain is usually absent.
- **`Cargo.lock` is committed source**, not a build artefact, and nothing regenerates it for you.

The shared cross-language conformance suite drives this crate's runner
(`src/bin/conformance-runner.rs`) through the same scenarios as every other language, on a
**current-thread** tokio runtime so a processor that blocked instead of awaiting deadlocks rather
than being rescued by a spare core:

```bash
./mvnw test -pl :parallel-consumer-proxy-conformance -am -Dpc.conformance.language=rust
```

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
