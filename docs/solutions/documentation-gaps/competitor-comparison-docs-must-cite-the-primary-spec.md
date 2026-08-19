---
title: Competitor-comparison docs must be sourced from the primary spec, not summary awareness
date: 2026-08-07
category: documentation-gaps
module: src/docs
problem_type: documentation_gap
component: documentation
severity: medium
root_cause: inadequate_documentation
resolution_type: documentation_update
applies_when:
  - Writing or reviewing a section that positions this project against a competing technology
  - A competitor ships a release, GA milestone, or version gate that the comparison already mentions
  - A comparison concedes a capability without citing where that limit is specified
  - Preparing README or docs changes that describe another system's semantics or guarantees
  - Reviewing docs that no test, CI gate, or build step can validate
symptoms:
  - Comparison prose paraphrases a competitor from general awareness with no citation to its spec
  - The doc concedes a whole motivating use case to the competitor and stops there
  - Checkable competitor limits (ordering, EoS, timeouts, state cost) are absent from the comparison
  - No test or CI gate fails, so the under-armed section survives indefinitely
  - Version and maturity gates in the comparison drift behind the competitor's releases
related_components:
  - development_workflow
tags:
  - documentation
  - readme
  - competitive-positioning
  - primary-sources
  - kip-932
  - share-groups
  - kafka
  - doc-decay
---

# Competitor-comparison docs must be sourced from the primary spec, not summary awareness

## Context

The README carries a section titled `When to use this library (vs KIP-932 Share Groups)`, positioning
Parallel Consumer against Kafka's broker-native queueing feature. It exists because Share Groups cover
a large part of what people historically reached for this library to do, and a reader arriving at the
project deserves an honest answer about which one they want.

That section had been authored from general awareness of Share Groups rather than from KIP-932 itself.
It summarised them as "many-to-many consumer↔partition mapping, per-message ack, broker-side delivery
counts with poison-message protection, elastic scaling decoupled from partition count. Unordered queue
semantics -- 'RabbitMQ on Kafka'", handed them the entire "partitions are fixed, I need more consumers"
motivation, and stopped. The reader-facing advice was two bullets: use Share Groups if you want
unordered queue semantics on Kafka 4.2+, use Parallel Consumer if you need key-level ordering with
concurrency beyond partition count.

Nothing in it was false. It was simply under-armed. It named the one axis the author already knew
(ordered vs unordered) and conceded everything else by silence.

The gap surfaced during a `ce-strategy` interview, when the user asked a question the doc could not
answer from its own sources: *"can you verify the 'random ordering' by reading the actual share groups
kip?"* Reading the primary spec turned up six further axes of difference -- five of them advantages the
doc had never claimed, one a concession it had never made -- all sitting unstated for as long as the
section had existed. The rewrite shipped in PR astubbs#223 (unmerged as of this writing), edited in
`src/docs/README_TEMPLATE.adoc` and regenerated into `README.adoc`.

### How it got there, and why the rule that would have caught it did not (session history)

The section was added on 2026-04-23 in commit `a597a384`, whose subject is
*"docs: rebrand audit (README template + pom) and landscape context"*. Its diff touches only
`README.adoc`, `RELEASE.adoc` and `src/docs/README_TEMPLATE.adoc`: it arrived as the second half of a
README rebrand pass -- fork notice, badges, copyright -- not as a piece of researched comparison work.
The prose names the KIP in its heading but **does not link it**, and the commit body argues entirely at
release-note altitude (Share Groups are GA, they cover most partition-count use cases) rather than from
mechanics.

The more useful finding is what happened next. On 2026-08-05, astubbs#208 wrote the counter-practice
down, explicitly:

> Categories to enumerate (each needs its facts checked at writing time, not taken from this issue)

> Check KIP-932's exact release status at writing time rather than trusting this issue - it has been
> moving through early access across the Kafka 4.x line, and the honest version of this page depends on
> which brokers actually have it.

That rule was written **over three months after** the text it would have corrected, and it was aimed at
the manual chapter the section would *become* rather than at the paragraph as it stood -- astubbs#208
says *"the README's `When to use` already carries the seed, so it migrates with everything else in step
1 and then gets extended"*. The comparison was then deferred out of the first docs-site phase in that
issue's 2026-08-06 discussion, on the reasoning that it *"may turn out to be mostly migrating what `When
to use` already says rather than new research"*.

That last sentence is the whole failure in one line: the published text was treated as adequate raw
material precisely because nobody had checked it against the spec. The weakness was written up as a
rule and parked as work; what nobody did was apply the rule backwards to the paragraph already shipped.

The rule existing in a plan is not the same as the rule reaching the text.

## Guidance

**Source every competitor comparison from the competitor's primary specification, and prefer claims
that are falsifiable and datable over adjectives.**

Concretely, when a doc positions this project against an external system:

1. **Read the spec, not the summary.** The KIP, the RFC, the release notes, the official config
   reference. Blog posts, conference talks, and your own memory of a feature announcement are all
   downstream of it and lossy in the direction that flatters the competitor's headline claim.

2. **Prefer claims with a name and a number.** `share.record.lock.duration.ms` defaults to 30s.
   `group.share.delivery.attempt.limit` defaults to 5. GA in Apache Kafka 4.2 / Confluent Platform
   8.2, KRaft only. Each of these can be checked by a reader in five minutes and can be *seen to age*
   when the competitor changes it. "Unordered queue semantics" can do neither: it is unfalsifiable
   prose that will read as current forever.

3. **Quote the spec where the spec is stronger than your paraphrase.** The pre-rewrite section said
   "unordered". KIP-932 actually says records "can be delivered out of order to a consumer, in
   particular when redeliveries occur", and that while offsets ascend within a single batch, there are
   "no guarantees about the ordering of offsets between different batches". The quoted text is a
   sharper claim than the summary word, and it is attributable.

4. **Cover the whole surface, not just the axis you already knew.** The rewrite's table has seven rows:
   ordering, exactly-once, slow processing, poison messages, broker cost, requirements, scaling axis.
   Only ordering was stated as a *difference* in the original prose. Poison messages and elastic scaling
   did appear there -- but as Share Groups features, with no counterpart named and no checkable fact
   attached, which is worse than omitting them. Exactly-once was a whole capability the original doc
   never mentioned the competitor lacks.

5. **Concede what is genuinely theirs, explicitly.** Share Groups can spread a *single* partition's
   throughput across several machines; Parallel Consumer scales up within one instance's assignment and
   cannot. The rewrite states this in the table ("Scaling axis: *Out.*" vs "*Up.*") and repeats it in
   the recommendation bullet. A comparison that concedes nothing reads as marketing and the reader
   discounts the whole table.

6. **Do not let comparison content ride along in an unrelated commit.** This section entered as
   "landscape context" appended to a rebranding pass. Content describing someone else's system needs
   its own change, with its own sources, or it inherits the research budget of whatever it was bolted
   onto -- which is zero.

7. **When you write the verification rule, apply it to what is already published.** astubbs#208 stated
   the rule correctly, then aimed it at the chapter the section would become. The already-shipped
   paragraph it called "the seed" was never re-checked -- and was in fact judged adequate to migrate
   as-is. A rule aimed only forward leaves every existing instance untouched, and a seed nobody
   verified propagates rather than gets fixed.

8. **Note the generated-file constraint.** In this repo the README is generated: edit
   `src/docs/README_TEMPLATE.adoc` and regenerate with `asciidoc-template:build`. Never hand-edit
   `README.adoc`.

## Why This Matters

Competitor-comparison documentation **degrades silently**. It is close to the only kind of
documentation with no failure detector at all.

- No test breaks when it is wrong.
- No CI gate fires. There is nothing to lint against, because the ground truth lives in another
  project's repository.
- No reviewer notices, because the reviewer's knowledge of the competitor is drawn from the same
  summary-level awareness that produced the text.
- The competitor ships a release and the doc gets *further* from true without anyone touching it.

The direction of the decay is not random. An under-sourced comparison always errs in the competitor's
favour, because the parts you did not read are the parts you cannot claim. The original section handed
Share Groups the whole "partitions are fixed, I need more consumers" motivation without qualification.
Reading the KIP showed that the motivating use case in this library's own README -- a slow consumer
calling a web service -- is one Share Groups handle badly: overrun the 30 second acquisition lock and
the record is redelivered to another consumer *while the first is still processing it*. That is not a
subtlety at the edge of the comparison. It is the central case, and the doc was silently conceding it.

Falsifiability is what makes the fix durable rather than one-off. A table full of config names, defaults
and version gates carries its own expiry date. A future reader (or agent) who checks
`share.record.lock.duration.ms` and finds the default has changed knows immediately that the row is
stale. Nobody can ever discover that "unordered queue semantics" has gone stale, which is precisely why
it survived unchallenged for over three months across at least two sessions that discussed it.

This is the same failure family as two existing lessons: *read linked content first* (citing an
unopened link produced four wrong artefacts on astubbs#125) and *mirrored issues: read upstream too*
(never trust a fork mirror's summary of an upstream issue) (auto memory [claude]). In all three, a
summary stood in for a primary source and the summary was confidently wrong in a way nothing detected.

## When to Apply

Apply when writing or reviewing:

- Any "X vs Y", "when to use this", "alternatives", or "comparison" section, in README, docs, or
  `STRATEGY.md`.
- Any claim about what an external system *cannot* do, or about limits, defaults, and version
  requirements you did not read in that system's own reference.
- Any positioning statement that a competitor's new feature supersedes part of this project.

Apply as a *review trigger*, not only at authoring time:

- When the compared system ships a major release. Share Groups went from early access in Apache
  Kafka 4.0 (explicitly not for production) to GA in 4.2, and any comparison written against 4.0 is now
  wrong about availability.
- When a comparison section is more than a release cycle old and has not been re-sourced.
- When someone asks a question the doc cannot answer from its own citations. That is the strongest
  signal available that the section was written from summary awareness. Here the trigger was literally
  "can you verify this by reading the actual KIP?"
- When you park comparison work for a later phase. Parking is fine; parking without recording that the
  *current* text is unverified is how a known weakness reads as settled content.

It does *not* apply to comparisons of things inside this repo, where tests and CI already keep the doc
honest, nor to subjective positioning ("simpler", "easier to adopt") where there is no spec to cite --
though the absence of a citable source is itself a reason to write less.

## Examples

**Before** -- `src/docs/README_TEMPLATE.adoc`, the entire reader-facing recommendation:

```asciidoc
[TIP]
====
* If you want unordered queue semantics on Kafka 4.2+, reach for *Share Groups*. The "partitions are fixed, I need more consumers" motivation is now solved at the broker.
* If you need *key-level ordering with concurrency beyond partition count*, reach for *Parallel Consumer*. Nothing else does that cleanly today.
====
```

Two bullets, one axis, no citations, nothing datable, and no mention that Share Groups have no
exactly-once at all.

**After** -- a `How they differ` table sourced from the KIP, of which four representative rows:

```asciidoc
| Ordering
| None guaranteed. Records in a share-partition "can be delivered out of order to a consumer, in particular when redeliveries occur", and only offsets *within a single batch* ascend -- there are "no guarantees about the ordering of offsets between different batches"
| Per key, per partition, or unordered -- your choice. See <<ordering-guarantees,Ordering Guarantees>>

| Exactly-once
| Not supported. There is no transactional read-process-write for share consumers
| Supported. Produced records and their source offsets commit as one atomic transaction -- see <<transaction-system,the EoS transaction model>>

| Slow processing
| Each record carries an acquisition lock (`share.record.lock.duration.ms`, default 30s). Overrun it and the record is redelivered to another consumer while you are still processing it
| A record stays in flight until your function returns. There is no clock

| Broker cost
| Per-record state is persisted to the internal `__share_group_state` topic through a share coordinator, plus acknowledgement RPCs
| None beyond an ordinary consumer group. State is client-side, piggybacked onto commit metadata -- see <<offset_map,the offset map>>
```

Note what each row now carries: a quoted spec line, a named config with its default, a named internal
topic. Every one of those is checkable by a reader and visibly ages.

**The concession, stated rather than omitted:**

```asciidoc
| Scaling axis
| *Out.* Several consumers may share one partition, so throughput can scale past the partition count across machines
| *Up.* One instance processes its whole assignment concurrently; add instances for availability as usual
```

and, in the recommendation, extending rather than deleting the original Share Groups bullet:

```asciidoc
* If you want unordered queue semantics on Kafka 4.2+, reach for *Share Groups*. The "partitions are fixed, I need more consumers" motivation is now solved at the broker -- and if you need a *single* partition's throughput spread across several machines, they are the only option.
```

**A precision worth copying:** the exactly-once row claims what the project actually delivers and no
more. The library's EoS is Kafka-to-Kafka -- produced records and their source offsets commit as one
atomic bulk transaction -- and the README says elsewhere that external side effects still require the
usual idempotency, exactly as with core Kafka EoS. Sharpening a comparison is an invitation to
overclaim; the guard is that your own side of the table must be as spec-sourced as theirs.

**And the guard failed, in the same change that wrote it down.** The `Broker cost` row first shipped
as: *"None beyond an ordinary consumer group. State is client-side, piggybacked onto commit
metadata."* Every word true, and incomplete in the flattering direction. What it omits is that the
commit metadata field is free-form and shared -- astubbs#118 exists because a Kafka Streams app,
another framework or operator tooling that previously owned the group can leave bytes there that PC
cannot decode. Share Groups have no equivalent exposure: `__share_group_state` is theirs alone. So on
that row they hold an isolation property PC lacks, while the row read as though the whole cost were
theirs. The corrected version names the real shape -- they pay load, PC pays isolation.

Two things this instance is worth remembering for. First, the failure was **asymmetric attention**,
not ignorance: the competitor's column was researched from the spec and our own column was written
from familiarity, which is the same summary-awareness failure pointed inward. Second, it was caught
only by reading what had merged to `master` while the branch was open -- the fix that proves the cost
had already landed. A comparison's own project moves too, and re-sourcing only the competitor's side
leaves half the table stale.

## Related

- astubbs#223 -- the PR that re-sourced the section from KIP-932 and added `STRATEGY.md`. Open and
  unmerged as of this writing.
- astubbs#208 -- the docs-site issue that stated this rule prospectively (*"each needs its facts checked
  at writing time, not taken from this issue"*) and named the README's `When to use` section as the seed
  to expand. It postdates the section by over three months and was aimed at the chapter that section
  would become, which is why the existing text went unchecked. Worth updating to record that the seed
  has since been sharpened.
- astubbs#222 -- the same move in a different medium: having established what the library provides over
  the competitor, make the advantage *measurable* rather than merely asserted.
- `docs/inflight/release-0600-blockers.md` -- the sibling concern for version claims: "are the things
  0.6.0.0 publishes true on the day we cut it?" Same class (nothing tests prose), different trigger
  (a fact changed underneath static text, rather than text that was under-sourced when written).
- `docs/inflight/next-docs-site.md` -- parks the KIP-932 chapter as future work and points at
  astubbs#208.
