# The Kafka Streams dispatch switch is JVM-wide, not per `KafkaStreams` instance

<!-- inflight-type: feature -->
<!-- inflight-impact: misdirection -->
<!-- inflight-state: deferred - an implementation exists and is unproven; see "What is already written" -->

`parallel-consumer-streams` decides whether a `StreamTask` dispatches through Parallel Consumer by
reading process-wide static state (`PcDispatchSwitch`), set by a system property or by a test. **Two
`KafkaStreams` instances in one JVM therefore cannot be configured differently** - the second one
silently inherits whatever the first arranged.

Shipped that way deliberately with the execution seam (astubbs/parallel-consumer#255). The seam
defaults **off**, so the JVM-wide reading is "this JVM opted in", which is a coherent thing for a
preview to mean; and there is no seam through `KafkaStreams` to hand a `StreamTask` a collaborator,
which is why the static exists at all rather than being an oversight. It stops being coherent the
moment anyone embeds two topologies with different requirements, which is ordinary in a service.

## What is already written, and what is missing

Branch `feats/streams-dispatch-streamsconfig-property` carries an implementation: a
`PcDispatchSettings` type read off `StreamsConfig`, with precedence *config > system property >
default*. It is **unfinished in the way that matters** - it has no test showing two `KafkaStreams`
instances in one JVM getting different settings, which is the entire claim. Every single-instance
test passes against the process-global design too, so the suite it has cannot distinguish the new
design from the old one.

Two things to know before picking it up:

- **The branch was cut from the feasibility study, not from wake-on-work**, so it predates
  `PcWorkSignal` and its diff against today's module *removes* the split poll wait. It is a merge,
  not a cherry-pick, and the patch must be reconciled as generated Java and re-derived - never merged
  as a patch file. `parallel-consumer-streams/bin/regen-patch.sh`'s header owns that procedure.
- **The missing test is a known blocker, not merely unwritten.** Three agents stopped at exactly that
  test on an API content filter. Wording it prosaically - two instances, two settings, assert each
  reads its own - is what gets through; describing it in terms of what it defeats does not.

## Why it was not folded into the seam

Judged at the seam rung: taking it would have meant reconciling three files and the patch against a
different base, and then either shipping the per-instance switch without its proof or blocking the
seam on the test that has already stopped three attempts. Neither is a minimal execution seam. The
seam's own default being off lowers the cost of waiting - nobody gets PC dispatch they did not ask
for while this is outstanding.

Related: the module README's "Turning it on, and why it is off" section points here, and
`PcDispatchSwitch`'s javadoc states why the static is the only thing that reaches the call site.
