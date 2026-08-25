# Branches that must re-check `STRATEGY.md` before they merge

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->


`STRATEGY.md` is a claims document, and unlike the README nothing tests it. The branches below either
**change what it should say** or **can falsify a claim already in it**. Each one should re-read the
named section as part of its own merge prep - not afterwards, when nobody is looking.

This file exists because the coupling runs the wrong way for tooling to catch: the work lives in
product code and spikes, the consequence lives in a root-level prose document, and no gate connects
them. `git rm` this file when the last branch below has merged and the doc reflects them.

Named here is *why* each branch touches the strategy, which no command can answer. For their status,
titles, or divergence, ask `gh` and `git`.

## Can falsify a claim already published

**`test/transactional-mode-battle-test`** - proves or falsifies every documented transactional
guarantee. `STRATEGY.md` ("Our approach") and the README's Share Groups table both rest on PC being
the only way to get exactly-once *together with* parallelism. If that suite falsified any documented
guarantee, the strongest claim in the comparison is the one that has to move, and the README table's
`Exactly-once` row moves with it. Read the branch's own inflight note mapping reported transactional
issues against what it proved.

Same family, same section: **`fix/transactional-produce-callback-abort`** and
**`fix/produce-lock-double-release`**. A transactional guarantee that needed a fix to hold is still a
guarantee that holds - but the doc should not claim it more strongly than the fixed code supports.
See [`bug-producing-lock-double-release.md`](bug-producing-lock-double-release.md).

**`research/kafka-streams-foreign-wrappers`** (astubbs#334) - measures what a windowed aggregation
actually costs across the boundary. The Kafka Streams section claims the aggregations, the windowing
and the state stores "never need to cross a boundary", and that **only** the user's per-record
function crosses. A hopping window calls the aggregator once per overlapping window, so if the
spike's multiplier holds, the crossing is per-overlapping-window rather than per-record and that
sentence needs qualifying - not deleting, since the same section now states that parity rather than
speed is the goal, which is what decides whether the cost is a failure or a price. The spike's plan
is `docs/plans/2026-08-25-001-feat-streams-windowed-aggregation-plan.md`; its result belongs here
whichever way it lands, including if it refutes the multiplier.

## Change who the product is for

**`feats/ks-on-pc-spike`** (astubbs#255) and **`feats/connect-on-pc-spike`** (astubbs#240, plus its
codex variant), with **`docs/assess-kafka-streams-pc-integration`** behind the first.

These are the largest strategic movers on the board. `STRATEGY.md`'s primary persona is a team whose
downstream scales further horizontally than their partitions do. Somebody reaching for Kafka Streams
or Kafka Connect does not describe themselves that way: they arrive with a topology or a sink
connector and one slow stage. If PC becomes the execution engine *underneath* another framework, then

- **"Who it's for"** gains a population the current sentence excludes, and
- **"Our approach"** gains a second clause - not only a library you add to a pom, but an engine other
  Kafka frameworks run on.

That is a larger change than anything in the Share Groups comparison, and it is a change of *kind*:
the client-side bet stops being about your application and starts being about the framework hosting
it. Whichever of these lands first should decide whether the persona widens or a second persona is
named, so the later one inherits a decision rather than reopening it.

**Watch the commit-metadata field as these progress.** PC keeps its state in the commit metadata
field, which is free-form and shared with anything else that has ever owned the group. astubbs#118
names Kafka Streams *first* among the things that leave bytes PC cannot decode - and the Streams
spike is putting PC underneath Kafka Streams. Today that exposure is a handled robustness issue and
belongs nowhere near `STRATEGY.md`: the crash is fixed, the recovery path works, and a guiding policy
should not carry a caveat for a risk that has not materialised. But if PC becomes the engine hosting
frameworks that also want that field, an isolation footnote turns into a question about whether the
client-side bet holds in the substrate role - and *that* is a claim in "Our approach". The trigger to
revisit is a spike hitting a metadata collision it cannot simply survive, not the spikes merging.

## Change what the tracks contain

**`feats/web-gui`** - the Observability track names a web GUI as an investment. Once the branch
lands, the doc is describing something that exists, and the track's wording should stop reading as
intent.

**`feats/health-check-api`** (astubbs#126) - same track. A health-check surface is the other half of
"you moved the queue into the client, so you owe the operator visibility".

**`features/enable-virtual-threads`** - Performance track. The doc's concurrency-ceiling story
currently rests on the non-blocking modules; virtual threads change what the core alone can reach.

**`docs/v6-release-ideas`, `docs/v6-module-maturity-table`** and the `next-` ideation notes beside
this file - these are strategy artefacts in their own right. A living roadmap of high-level themes
overlaps `STRATEGY.md`'s **Tracks**; per-module maturity and what pre-1.0 reserves overlap
**Milestones**, which `STRATEGY.md` currently omits entirely. The risk is not contradiction, it is
two documents owning the same question. Settle the division of labour when the first of them merges.

**`docs/plans/2026-08-10-001-docs-testing-evidence-plan.md`** - promoting the test suite from hygiene to a positioning
asset is a strategy-level move, not a chore. If it holds, Reliability is no longer only an internal
track.

## Narrows an argument the comparison leans on

**`feat/java-17-baseline`** (Java baseline + Kafka 4). The README argues that Share Groups need KRaft
and Kafka 4.2, so 3.x estates cannot reach them at all - with the unstated premise that PC can. As
PC's own floor rises that gap narrows from both ends. The argument does not disappear, but it stops
being free and needs restating in terms of what PC still supports.

## Explicitly not triggers

Correctness, CI and hygiene branches make the Reliability and Performance tracks *true* without
changing what the document says. A strategy doc that moved for those would be a changelog. That
includes the offset-encoding, logging, load-factor, MDC, long-polling, test-dedup, plugin-pinning,
issue-automation and mirror-sweep work, and the JStream buffer bound.
