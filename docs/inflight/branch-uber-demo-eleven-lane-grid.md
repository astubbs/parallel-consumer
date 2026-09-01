# The uber demo: eleven language clients, one workload, one view

Carried by `feats/polyglot-demos` (astubbs/parallel-consumer#331), which is the work this designs
the successor to. It had a branch and a draft PR of its own - astubbs/parallel-consumer#332, closed
as superseded - and that was over-engineering: a design nobody needs to review separately from the
work it follows does not need a rung in the stack, and this one had acquired two PRs stacked above
it. **Design only - nothing implemented, and three decisions are open below. Do not start until
they are settled; each one changes what gets built.**

## Prior art, which reshaped this before any code

[`next-polyglot-demo-app.md`](next-polyglot-demo-app.md) points at ideas 21-27 in
[`docs/ideation/2026-08-14-language-proxy-interaction-model-ideation.html`](../ideation/2026-08-14-language-proxy-interaction-model-ideation.html)
- seven ranked directions for "a demo that runs all eleven language bindings at once and proves in
one view that they read the same records". It says the eleven-language grid belongs to whichever
idea wins, and that the per-language demos on astubbs/parallel-consumer#331 are a deliberate narrow
cut of idea 23, not the grid. It also says to read the ideation before designing any demo,
orchestration, aggregation or perf-display work, because it records what was rejected and why.

**Idea 21, "the skateboard stack"** (ranked first, 85% confidence, medium complexity) is the shape
the owner described: run the consumption work and report one cohesive view, *not* eleven existing
demos in sequence. Its four pillars: each language's demo is its conformance runner in costume; each
worker's Success report carries a receipt `{record_id, language, offset, attempt, payload_checksum}`
onto a `demo-observations` topic; one TUI aggregator consumes that topic, renders eleven lanes and
machine-asserts sameness, ending "11/11 languages converged at offset N"; CI records the run as an
asciinema cast.

**Idea 22, "one consumer group across eleven runtimes"**, is a different demo that this design keeps
confusing with 21 - see decision 2.

## Verified, not assumed

**The receipts route exists.** `proxy.proto` carries `repeated ProduceRecord produce` in the worker's
report, commented as "the sanctioned route for worker output", at-least-once. So receipts ride the
protocol's own output path: Kafka becomes the aggregation bus, sameness is *computed* rather than
eyeballed, and nothing parses eleven demos' stdout - which is the orchestration shape the owner
ruled out.

## What already exists, that did not when idea 21 was written

| pillar | state |
|---|---|
| conformance runners for all eleven, over production gRPC | exists |
| **demo binaries that already spawn a real sidecar against a real broker** | exists, from astubbs/parallel-consumer#331 - see decision 1 |
| cpp and swift runnable off-Linux | exists - `bin/run-conformance-in-container.sh` |
| a shared broker | exists |
| fixed, parseable table shape, with `records`/`keys` deterministic across languages | exists |
| arms that name the product (`pc-<lang>-grpc`), so a row identifies itself | exists |
| receipts, aggregator, cast | the new work |

## Decision 1: which binary is the base

An earlier version of this note said the expensive gap was "give the eleven conformance runners a
real-sidecar flag". **That is stale.** Idea 21 was written before astubbs/parallel-consumer#331, and
every language now has a demo binary that already spawns a real sidecar against a real broker. The
costume exists; it is just called a demo rather than a runner.

- **Demo binaries as the base.** The hard per-language part - real sidecar, real broker, container
  that works - is done. The work is adding receipts. The baggage: they seed their own backlog and
  print their own tables, neither of which a grid wants.
- **Conformance runners as the base.** Frozen, observable, already machine-checked. The work is a
  real-sidecar flag *and* receipts, in eleven languages.

Leaning: **demos**, because the expensive half is already paid for.

## Decision 2: the consumer-group topology, which decides what the demo CLAIMS

The two are not variants of one demo. Only the first supports the convergence line.

- **One topic, eleven separate consumer groups (idea 21).** Every language reads *every* record, so
  per-record checksums are comparable and "11/11 converged at offset N" is a real assertion. The
  claim is **"eleven runtimes read the same records identically"**.
- **One topic, one shared consumer group (idea 22).** The eleven *split* the records and rebalance
  across languages. No per-record comparison exists. The claim is **"eleven runtimes cooperate in one
  group"** - which is arguably the more surprising thing to show, and is a different demo.

**Consequence either way:** the demos currently seed their own backlog, so eleven against one topic
means eleven backlogs unless exactly one seeds. A "who seeds" role is needed. Small, but it is a
contract question rather than wiring.

## Decision 3: does the grid show a rate at all

**Eleven runtimes on one host, at once, cannot produce quotable throughput.** This project has
already discarded an entire fan-out's figures for exactly that - the box ran at load 20-113 and every
agent was instructed to refuse to report rates.

- **Deterministic columns only** - records, keys, offsets, checksum agreement, convergence. Immune to
  contention, and it is idea 21's actual claim.
- **Rates too**, which forces sequential runs on a quiet host and stops it being one live view.

[`next-perf-comparison-matrix.md`](next-perf-comparison-matrix.md) owns measurement semantics and the
blessed-numbers pipeline by prior agreement recorded in both docs; this track owns app, UI and
narrative. So the speed table is that track's artifact. Leaning: **no rates in the live grid.**

## Smallest first step, once the decisions land

Not the TUI. Take **two** languages - one that runs natively here and one that needs the container,
so the awkward case is in from the start - have them emit receipts to one topic, and get an
aggregator to say "2/2 converged at offset N". If two lanes converge, the remaining nine are
transcription. Building the UI first produces a beautiful view of one language.

## What must not be done

- **Do not build the grid by parsing eleven demos' stdout.** Ruled out by the owner, and it inherits
  every per-language rendering difference as a parsing problem. Receipts make sameness computable.
- **Do not print a rate from a parallel run**, however good the wide table looks.
- **Do not take audience input as workload.** The ideation records a keynote mode rejected for
  violating the no-visitor-input security posture; that rejection stands.

## Cross-references

- [`next-polyglot-demo-app.md`](next-polyglot-demo-app.md) - the ideation pointer, the
  perf-track split, and the instruction to choose an idea and take it through brainstorm to a plan
- [`parked-demo-gallery.md`](parked-demo-gallery.md) and
  [`branch-polyglot-demo-ideation.md`](branch-polyglot-demo-ideation.md) - adjacent demo thinking
- [`branch-polyglot-demos.md`](branch-polyglot-demos.md) - the eleven demos this stacks on, and what
  the fan-out found
- [`next-demo-testing-infrastructure.md`](next-demo-testing-infrastructure.md) - the ranked ambition
  for the demo harnesses
- `parallel-consumer-proxy/demo/README.md` - the contract the eleven demos keep
- `proxy.proto`, `ProduceRecord` - the sanctioned worker-output route the receipts would ride
