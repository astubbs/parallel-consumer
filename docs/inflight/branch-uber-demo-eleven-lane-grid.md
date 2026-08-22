# The uber demo: eleven language clients, one workload, one table

Branch `feats/uber-demo-all-languages`, stacked on `feats/polyglot-demos`
(astubbs/parallel-consumer#331). **Design only - no implementation yet.**

## This is idea 21, and it was already ranked first

Prior art, read before designing anything here, and it changes the shape:
[`next-polyglot-demo-app.md`](next-polyglot-demo-app.md) points at ideas 21-27 in
[`docs/ideation/2026-08-14-language-proxy-interaction-model-ideation.html`](../ideation/2026-08-14-language-proxy-interaction-model-ideation.html)
- seven ranked directions for "a demo that runs all eleven language bindings at once and proves in
one view that they read the same records". That note also says the eleven-language grid belongs to
whichever idea wins, and that the per-language demos on astubbs/parallel-consumer#331 are a
deliberate narrow cut of idea 23 rather than the grid.

**Idea 21 - "the skateboard stack"** is the one the owner's framing describes: run the consumption
work and report one cohesive table, *not* eleven existing demos in sequence. Confidence 85%,
complexity medium, and its four pillars are:

1. **Each language's demo is its conformance runner in a costume.** The runner already speaks
   production gRPC and already prints a frozen observation line per delivery. The demo is that same
   binary with a real-sidecar flag and a stats printer.
2. **Each worker's Success report carries a receipt** - `{record_id, language, offset, attempt,
   payload_checksum}` - produced through the protocol's only sanctioned output route onto a
   `demo-observations` topic. **Kafka is the aggregation bus**: no log-shipping fabric, and the
   demo's own plumbing dogfoods terminal produce and the epoch fence.
3. **One TUI aggregator container** consumes the receipts topic, renders eleven lanes, and
   *machine-asserts sameness*: matching checksums per record, ending in one line -
   **"11/11 languages converged at offset N"**.
4. CI records the run as an asciinema cast.

The reason this beats orchestrating eleven demos is pillar 2: the languages do not each report a
private table that something has to reconcile. They all write receipts to one topic, and sameness
becomes a *computed* claim rather than a visual one.

## What exists now that did not when idea 21 was written

| pillar | state today |
|---|---|
| conformance runners, all eleven, over production gRPC | **exists** |
| cpp and swift runnable off-Linux | **exists** - `bin/run-conformance-in-container.sh`, added on astubbs/parallel-consumer#331 |
| a shared broker to point everything at | **exists** |
| a fixed, parseable table shape (`arm │ records │ keys │ elapsed │ msg/s │ vs AK core`) | **exists**, and `records`/`keys` are deterministic across languages - which is what makes any cross-language claim checkable |
| arms that name the product (`pc-<lang>-grpc`) | **exists**, so a row identifies itself |
| receipts topic, TUI aggregator, asciinema cast | **the actual new work** |

## The one real gap, and it is not the UI

**The conformance runners are driven with a SHIM, not a real sidecar.** `SidecarShim` writes a script
that announces a bare `port:` and holds stdin, because the engine lives in the suite's own JVM where
the harness can observe dispatch. That is deliberate and documented - it makes the client exercise its
real spawn-and-reap path without adding a connect-to-an-existing-port option to an API that binds
eleven languages.

For the demo, the runner must instead spawn a **real** sidecar against a **real** broker. That is
idea 21's "real-sidecar flag", and it is the whole difference between a conformance runner and a demo
runner. It is a per-language change to eleven runners, which makes it the expensive pillar - not the
TUI.

Worth knowing before starting: the demo containers already prove the shape works. Each one runs a
language client *and* a JVM sidecar it spawns, co-located, because the client owns the sidecar's
lifecycle (KTD41). The uber demo is eleven of those against one broker, plus a twelfth container that
reads receipts.

## The measurement trap, which must be decided before any number is shown

**Eleven runtimes on one host, at once, cannot produce quotable throughput.** This project has
already discarded an entire fan-out's figures for exactly that reason - the box ran at load 20-113
and every agent was told to refuse to report rates. A grid that runs all eleven simultaneously and
prints msg/s is a grid that prints noise, attractively.

Two honest shapes, and the choice is the owner's:

- **Sameness grid (parallel).** All eleven at once. Show records, keys, offsets, checksum agreement,
  convergence - the *deterministic* columns - and no rates at all. The claim is "they all read the
  same records", which is idea 21's actual claim, and it is unaffected by contention.
- **Speed table (sequential).** One language at a time on a quiet host, rates quotable, but it takes
  eleven times as long and stops being one live view.

Note that `next-perf-comparison-matrix.md` owns measurement semantics and the blessed-numbers
pipeline by prior agreement; this track owns the app, the UI and the narrative. So the speed table is
**that** track's artifact, and the uber demo should show sameness. Wanting both in one view is how
the numbers get quoted out of a contended run.

## Smallest first step, if picked up

Not the TUI. Take **two** languages - one already native here and one that needs the container
(so the awkward case is in from the start) - give their conformance runners the real-sidecar flag,
point both at the shared broker, and have them write receipts to one topic. If two lanes converge
and the aggregator can say "2/2 converged at offset N", the remaining nine are transcription. Doing
the UI first produces a beautiful view of one language.

## What must not be done

- **Do not build the grid by parsing eleven demos' stdout.** That is the orchestration shape the
  owner ruled out, and it inherits every per-language rendering difference as a parsing problem.
  Receipts on a topic make sameness computable instead.
- **Do not print a rate from a parallel run**, however tempting the wide table looks.
- **Do not take audience input as workload.** Idea 21's neighbours include a keynote mode that was
  rejected for violating the recorded no-visitor-input security posture; the rejection is in the
  ideation doc and stands.
