# The shared conformance suite exists and Go is wired; four wave-one languages are not

The suite is `parallel-consumer-proxy-clients/parallel-consumer-proxy-conformance/`, and its
`README.md` is the runner contract every language implements - flags, exit codes, the stdout
observation line, the behaviour tokens, and how a language registers itself. Read it before writing
a runner; nothing below repeats it. astubbs#242, upstream confluentinc#154.

This note exists because the suite's value is *agreement between clients*, and with one language
wired it currently proves only the mechanism.

## Covered

- Four scenarios, all four the harness serves today, all passing for **Go**:
  `a-processed-record-advances-the-committed-offset`,
  `an-unreported-record-holds-back-the-commit`,
  `a-failed-record-is-redelivered-with-its-failure-history`,
  `records-sharing-a-key-share-a-shard-distinct-keys-run-concurrently`.
- Every one proven able to fail, by sabotaging the runner and watching the suite name what was
  wrong. Two of those sabotages found real weaknesses rather than confirming strength - see below.
- The suite's own controls: an absent runner fails rather than skips, a crashed runner fails with
  its exit status and output, and two languages run at once are judged separately with the overlap
  measured from the test's own clock.
- No Docker, no broker, no Testcontainers: `./mvnw test -pl :parallel-consumer-proxy-conformance -am`
  is about ten seconds in the ordinary unit-test lane.

## Not covered, and why

- **Four of eleven scenarios.** The other seven are named in the client-authoring guide's §7 and the
  harness does not serve them yet - the liveness lease, the manifest reconnect, worker death,
  terminal outcomes and the shutdown drain are *un-negotiated* rather than passing. They arrive with
  the engine units. `ScenarioCoverageTest` fails the build if the harness grows one and nothing
  drives it, so this cannot rot quietly.
- **The mock lane only.** Committed offsets are read from a mock consumer's commit history. Against
  a real broker the same assertions come from the Kafka **Admin API**
  (`listConsumerGroupOffsets`) plus a verification consumer; nothing on the client side of the
  contract changes with it.
- **Deliberately bounded to the client/sidecar contract.** Engine internals - the offset-map codec,
  shard selection, the commit machinery - are not re-tested through language clients. The filter is
  in the README: *could a conforming client fail this scenario while the engine is correct?*

## What each remaining language needs

Identical work in each, and the registry entries are already sketched as comments in
`LanguageRunners.java`:

| Language | Runner lives at | Built by |
|---|---|---|
| Python | `…-client-python/`, a console script or `python -m` shim | whatever `make build` drops |
| TypeScript | `…-client-typescript/` | `npm run build`, then a node entry point |
| Rust | `…-client-rust/` | `cargo build --bin conformance-runner` |
| Ruby | `…-client-ruby/`, an executable script | `bundle install`, or nothing |

Per language: implement the contract (six flags, three exit statuses, both observation lines, five
behaviour tokens, the fixed literals), add one registry entry, run it, **prove each scenario red**,
and record the evidence in that module's `docs/data/testing-evidence.d/` fragment. Nothing in the
conformance module changes - the scenarios, assertions and driver are already language-blind.

Swift, C++, .NET and Scala are out of wave one and are not registered.

## Two things the red-then-green pass found, which a new runner must not re-break

- **`report-nothing` needs its 3s hold, or the negative control is not a control.** Without it the
  runner exits the instant the record arrives, and a runner that *wrongly reported success* has its
  report killed in flight by the process exit. The suite then sees an unadvanced offset either way.
  Measured: sabotaging that behaviour to report success left the suite **green** until the hold
  existed.
- **The key-ordering scenario's instrument is the hold, not the transcript.** Removing the hold
  leaves every one of its assertions still true, because the engine dispatches both shards in one
  wave regardless. What actually turns it red is a client that can only run one record at a time - a
  mutex around the whole processor, the shape of Ruby's `SizedQueue`-on-the-transport-thread and
  Rust's blocking-in-an-executor hazards. Ruby and Rust runners should expect this scenario to be
  the one that catches them.

## The shim, and what would delete it

The suite hosts the engine in its own JVM and hands the runner a four-line shell script that prints
`port: <n>` and holds stdin - because the client libraries' only entry point spawns a sidecar, and
adding a connect-to-an-existing-proxy option to eleven APIs for a test's convenience is a surface
decision owned by the protocol specification and the authoring guide, not by a test module. If that
option is ever added for its own reasons (a sidecar container in Kubernetes is the obvious one), the
shim is the thing to delete and `--sidecar` becomes `--port`.

## Related

- `docs/inflight/parked-testing-as-a-feature-for-the-clients.md` - why this is a product feature and
  not a contributing-guide footnote, and the standard the evidence entries have to meet before the
  strong version of the claim can be published.
- `parallel-consumer-proxy/docs/client-authoring-guide.md` §7 - owns the scenario names and what each
  asserts. The conformance module's README owns the runner mechanics only.
