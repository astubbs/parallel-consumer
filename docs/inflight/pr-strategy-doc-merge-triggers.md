# Branches that must re-check `STRATEGY.md` before they merge

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->
<!-- inflight-vetted: 2026-09-07 - checked every branch named here against `git branch -r` and `gh pr list`. Still live: `feats/ks-on-pc-spike` (astubbs#271 OPEN), `feats/connect-on-pc-spike` (astubbs#269 OPEN), `docs/assess-kafka-streams-pc-integration` (no PR), `feats/web-gui` (astubbs#268 OPEN), `feats/health-check-api` (astubbs#226 OPEN), `feat/java-17-baseline` (no PR), and the docs-testing-evidence plan. Removed as spent: `fix/transactional-produce-callback-abort` (merged as astubbs#261) and `features/enable-virtual-threads` (astubbs#51 CLOSED, branch gone). The v6-docs entry is rewritten - astubbs#273 merged as `docs/data/roadmap.yaml` and `STRATEGY.md` still has no Milestones section and no mention of it -->


`STRATEGY.md` is a claims document, and unlike the README nothing tests it. The branches below either
**change what it should say** or **can falsify a claim already in it**. Each one should re-read the
named section as part of its own merge prep - not afterwards, when nobody is looking.

This file exists because the coupling runs the wrong way for tooling to catch: the work lives in
product code and spikes, the consequence lives in a root-level prose document, and no gate connects
them. It stops earning its place when `STRATEGY.md` reflects every trigger listed below - which is a
claim about the document's contents, not about anything merging.

Named here is *why* each branch touches the strategy, which no command can answer. For their status,
titles, or divergence, ask `gh` and `git`.

## Can falsify a claim already published

<!-- post-merge: checked-begin - names the PR rather than the branch, which is deleted on merge, and
     states the suite's arrival in the past tense -->
**astubbs#262** - proves or falsifies every documented transactional guarantee. `STRATEGY.md` ("Our
approach") and the README's Share Groups table both rest on PC being the only way to get exactly-once
*together with* parallelism. It refuted two of them against a master that lacked astubbs#257 - at
`batchSize >= 2` the consumer stalled outright - and both read `PROVED` again with that fix merged in,
so the headline stands rather than moves. `STRATEGY.md` already says so; the standing trigger is the
register, not this note - a claim refuted later still moves the README table's `Exactly-once` row. The
register is `TransactionalClaim` and `TransactionalClaimCoverageTest` under
`parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/`, which fail the build when a claim is
recorded as covered with no test behind it, or when a recorded sentence leaves the file it was quoted
from.
<!-- post-merge: checked-end -->

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

**`docs/data/roadmap.yaml`** - the v6 release documentation, landed as astubbs#273 (branches
`docs/v6-release-ideas` and `docs/v6-module-maturity-table` are gone). This is a strategy artefact in
its own right: a living roadmap of high-level themes overlaps `STRATEGY.md`'s **Tracks**, and
per-module maturity and what pre-1.0 reserves overlap **Milestones**, which `STRATEGY.md` still omits
entirely. The risk is not contradiction, it is two documents owning the same question. **The trigger
fired and the decision was not made** - `STRATEGY.md` does not mention the roadmap and has no
Milestones section, so the division of labour is now an open question against master rather than a
thing to settle at a merge.

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
