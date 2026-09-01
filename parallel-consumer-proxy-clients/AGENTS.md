# `parallel-consumer-proxy-clients/` - two kinds of coverage, and which one your change needs

Rules for working inside the client collection. The root `AGENTS.md` routes here; this file owns the
part that binds anyone adding to a client or its demo.

## There are TWO test suites over this code, and they do not overlap

Knowing which one a change needs is the point of this file, because the easy mistake is to assume
the other one already covers you.

| | **Cross-language conformance** | **Demo output conformance** |
|---|---|---|
| lives in | `parallel-consumer-proxy-conformance` | `bin/ci-demo-conformance.sh` |
| binds to | the client library's **API**, called directly | the demo's **standard output** |
| runs against | the in-JVM `ProxyHarness` with mock Kafka clients | a real broker, a real spawned sidecar, a real container |
| proves | that every client means the same thing by the protocol | that every demo still *behaves* the same to a reader |
| cannot see | whether a demo is readable, or runs at all | whether the sidecar arm truly uses the client library |

**Neither is a substitute for the other, and each has already caught what the other structurally
cannot.** Conformance could not see that seven demos silently dropped six of seven configuration
dials, because it never runs a demo. The output suite could not see that the direct and gRPC
transports disagree about `enable.auto.commit`, because that transport's conformance test injects
its own consumer and never reaches the code path that builds one.

## The rule: a feature that reaches the demo needs an assertion in the output suite

**When you add a feature to a client or the proxy that a user can observe, add the assertion that
proves it - in the suite that can see it.**

- The feature changes the client's **API or protocol behaviour** -> a conformance scenario.
- The feature changes **what a demo prints, accepts, or exits with** -> an assertion in
  `bin/ci-demo-conformance.sh`, and the contract entry it enforces in
  `parallel-consumer-proxy/demo/README.md`.
- It does both -> both. That is common and not a sign you have over-tested.

This is a rule because **nothing enforces it**. A demo can gain a flag, a column or a mode that no
check ever looks at, and the first sign of trouble is a reader finding it broken. Every defect the
output suite has found so far was of exactly that shape.

### What the output suite is cheap at, and why that matters

It asserts on stdout, so it needs no knowledge of any language. Adding a language costs nothing, and
adding an assertion covers eleven implementations at once. It also uses the languages as **each
other's oracle**: outputs are reduced to a skeleton with the volatile figures discarded, and the
skeletons must match. There is no expected-output file to maintain, and the check gets *stronger*
with each language rather than more expensive.

So the bar for adding an assertion there is low. Take it.

## Keep the console and the assertions apart

**Two audiences, two channels, and mixing them makes both worse.**

- **The console is for a person.** The first line names the product; the tables are the point; the
  broker is quietened to `WARN` so it does not bury them. Do not put machine-readable diagnostics
  there because a test wants them.
- **The technical record goes to a file.** A demo may write a per-record ledger - identifier, key,
  partition, offset, attempt, received and completed timestamps - and assertions run against *that*.
  It is off by default and switched on by a flag.

That split is what lets the assertions get more detailed over time without the demo getting worse to
watch. Things worth asserting from a ledger that would be noise on screen:

- **overlap**: records whose processing intervals overlap prove concurrency actually happened,
  rather than being inferred from a rate;
- **order**: with a known publish order and key format, per-key sequence can be checked directly;
- **exactly once through the demo**: distinct `(partition, offset)` equals the processed count, so
  nothing was redelivered;
- **attempts**: with a failure percentage configured, that every record eventually succeeded.

## Performance baselines: wide bands, and only for catastrophe

A per-language baseline is worth having, with **a great deal of headroom** - it is there to catch a
change that makes something ten times slower, not to police percentages.

**Be careful what you promise here.** Measurements on this project have not yet been shown to
reproduce *across sessions*: the Java seed's serial arm moved from ~345 to ~300 msg/s on one machine
with the cause unexplained and the obvious candidate refuted by a control arm
([`docs/inflight/next-demo-seed-followups.md`](../docs/inflight/next-demo-seed-followups.md), item 1).
Until that is understood, a tight band would be a flake generator. A band wide enough to be honest
is still worth having; a tight one is worse than none.
