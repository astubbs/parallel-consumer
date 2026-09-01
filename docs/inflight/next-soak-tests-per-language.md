# Soak tests for every client library: leaks, and performance that holds

Candidate work, recorded 2026-08-22. **Not started.** One soak lane per client library, running long
enough to expose what a short run cannot: resources that grow, and throughput that decays.

## Why this is not speculative

The demo fan-out already produced a defect of exactly this shape, from a run of **twenty records**:
the Java demo prints its tables and then never exits, because non-daemon engine threads outlive the
arms that made them ([`bug-java-demo-hangs-on-exit.md`](bug-java-demo-hangs-on-exit.md)). That is a
resource leak - it only looks like a hang because the leaked resource happens to be a thread that
blocks JVM shutdown. A leak of anything the JVM does *not* wait on (a buffer, a consumer, a socket,
an offset map) produces no symptom at all in a short run, which is precisely the case for everything
the current suites exercise.

So the question this lane answers is not "might there be leaks" - one is already open. It is **how
many of that class are currently invisible**, in eleven libraries, none of which is run for longer
than a few seconds by anything that exists today.

## What it has to measure, per language

- **Resources that should return to baseline**: live threads, open file descriptors and sockets,
  heap after a forced collection, and the sidecar child process count. Sampled across many
  create/use/close cycles of the client, not once at the end - the shape of the curve is the finding,
  and a single end-of-run reading cannot tell a leak from a high-water mark.
- **Throughput that should not decay.** A rate that starts at X and finishes at 0.6X over an hour is
  a defect the comparison demo structurally cannot see, because it reports one number for one short
  replay. Report first-decile against last-decile, not an average, which hides exactly this.
- **Offsets and correctness under duration**: records processed equals records produced, keys stay
  distinct, and nothing is committed twice. The deterministic `records`/`keys` pair the demos already
  compute is the cheapest available oracle and should be reused rather than reinvented.

## Constraints worth settling before starting

- **It cannot live in the default suite.** A lane that takes an hour cannot gate a pull request. The
  repo already has the machinery for this decision - `bin/performance-test.sh` and the self-hosted
  highcpu runner ([`docs/self-hosted-runner.md`](../self-hosted-runner.md)) - so the question is
  which of those it joins, not whether to build a third thing.
- **A soak result on a loaded box is worthless**, and this project has already paid for that lesson:
  every throughput figure from the eleven-agent fan-out was discarded because the machine ran at load
  20-113. A soak lane that does not assert an idle host will produce numbers nobody may cite, which
  is worse than producing none.
- **Leak thresholds must be stated per language, not shared.** A JVM's heap after GC, a Go runtime's
  goroutine count and a Ruby process's RSS are not comparable quantities, and a single cross-language
  threshold would be either meaningless or permanently red somewhere.
- **This is the third coverage**, alongside the two in
  [`parallel-consumer-proxy-clients/AGENTS.md`](../../parallel-consumer-proxy-clients/AGENTS.md) -
  the API-level conformance suite and the demo output-conformance check. That doc should name it
  once it exists, and say what it catches that neither of the others can.

## Related

- [`next-demo-testing-infrastructure.md`](next-demo-testing-infrastructure.md) - the ranked ambition
  for the demo harnesses; this is the durability half of it.
- [`next-demo-seed-followups.md`](next-demo-seed-followups.md) item 1 - the unexplained AK core
  baseline shift (~345 to ~300 msg/s between sessions, a control arm refuted the obvious cause). A
  soak lane that reports sustained throughput is one of the few things that could turn that from an
  anecdote into a measurement.
