# Next: a landing page, separate from the documentation site

<!-- inflight-type: feature -->
<!-- inflight-impact: coordination -->

Opened 2026-08-21. **Related to the docs site ([`parked-docs-site.md`](parked-docs-site.md),
astubbs#208) but deliberately a different artifact**: that one is reference material for people
already using the project; this one is for people deciding whether to.

The prompt was reading a competitor's marketing
([`market-analysis-llingr.md`](market-analysis-llingr.md)), which is unusually good at persuading
engineers, and worth learning from as *craft* regardless of the competitive angle.

## What llingr's site does that works, and why

- **A "Technology" page that is genuinely technical.** Cache-line field ordering, false-sharing
  padding, store-buffer locality, branch prediction, bitwise shard selection because shard count is a
  power of two, timer reuse to avoid `time.After()` allocations. It opens with the Knuth quote about
  premature optimisation and then argues message processing sits in the critical 3%. **The effect is
  trust**: it reads as written by someone who did the work, and it is unfakeable in a way adjectives
  are not.
- **A Correctness page at top-level navigation**, not a README paragraph - see
  [`parked-testing-as-a-feature-for-the-clients.md`](parked-testing-as-a-feature-for-the-clients.md).
- **The problem framed as an economic one before a technical one.** "The Partition Ratchet" -
  partitions can only be added, never removed; capacity planning and multi-team coordination are the
  real recurring cost. **Cited to third parties** (Confluent's own analysis of hidden operational
  costs, Shopify on BFCM preparation) rather than asserted.
- **Before/after side by side**, in prose an engineer can check: one partition feeds one consumer
  thread; at 20ms per message that is 50 messages/second; the only conventional fix is more
  partitions.
- **A live widget**, not a screenshot - a running chaos/scaling counter showing messages, scaling
  events, reassignments, duplicates, out-of-order.
- **Numbers with units everywhere**, and specific configuration alongside them.
- **Naming the failure boundary out loud** - graceful rebalance produces zero duplicates,
  catastrophic failure produces duplicates bounded by in-flight count. Volunteering the limit reads as
  confidence.

## The rule for how any of this is written

**State what PC does. Never state what a competitor lacks.** Recorded as a standing rule by the owner,
2026-08-21, and it applies to the promotional site, the README, the docs site, issue replies and
release notes alike.

Worked rewrites, so the rule is concrete rather than a sentiment:

| Not this | This |
|---|---|
| "Competitors only offer per-key ordering" | **"Three ordering modes: per partition, per key, or unordered. Pick the guarantee your workload actually needs."** |
| "Others dead-letter on the first failure" | **"Retries with configurable delays, custom backoff and a max-retry escape - a transient downstream failure doesn't become a dead letter."** |
| "Others can't produce" | **"Read, process and produce in one step, with the produce tied to the offset commit - including transactional exactly-once."** |
| "Others are single-topic" | **"Subscribe to many topics, or a pattern."** |
| "Others stall behind one slow record" | **"Commits move past in-flight work: one slow record never holds up everything behind it, and a rebalance doesn't redo the work that already finished."** |

Three reasons this is the better copy, not merely the politer copy: a reader who has not heard of the
competitor learns nothing from a negative claim; a negative claim ages badly the moment they ship the
feature; and the positive form states a capability the reader can check against their own workload.

The last row is the most valuable and the hardest to render - see the offset-map item below.

## What we would put there that llingr cannot

Not a copy of llingr's site. The material we have and llingr does not:

- **Years of production history**, and an Apache-2.0 licence with no patent claim and no licence key.
  See the licence comparison in the competitor note - for many readers this is the whole decision.
- **The offset-map story, told visually.** PC commits *past* gaps, so a single slow key does not hold
  a partition's progress and a rebalance does not redo the completed work behind it. That is the
  strongest differentiator and it is currently invisible - it should be an animation, not a paragraph.
- **The honest-comparison charter** (`parked-testing-as-a-feature-for-the-clients.md`): publishing the
  case we expect to lose. Almost nobody does this, and it is disarming.
- **Adaptive concurrency**, once it exists - llingr's docs tell users to size `ConcurrentKeys`
  against a database connection pool, which is precisely the argument for discovering it instead.

## Constraints

- **No public comparison against llingr**, per the owner's decision recorded in the competitor note.
  Learn from the craft; do not name llingr, and do not position against it.
- Same substrate problem as the docs site: astubbs#208 is parked with platform and domain undecided,
  and a live widget needs a runtime host rather than a static generator. The demo gallery
  ([`parked-demo-gallery.md`](parked-demo-gallery.md)) has the same blocker and would share the
  solution.

## Say the quiet capabilities out loud - starting with offset gaps

**Recorded 2026-08-21.** A competitor promotes its handling of *"control record gaps, transaction
boundary gaps, and log compaction gaps - realities that most consumer libraries ignore"* as a headline
correctness claim. PC handles all three, has done for years, and **says so nowhere a user would
look.**

That asymmetry is the general lesson, and offset gaps are just the clearest instance: **PC has
capabilities it treats as obvious and therefore never states.** Obvious to the person who built it is
not obvious to someone evaluating it, and an unstated capability loses to a stated one every time.

**Lead with the plain claim, then explain.** Owner's direction, 2026-08-21: a user scanning for
whether a library fits will not decode a design property, but they will recognise their own problem.
So the headline is the capability and the mechanism is the second sentence:

> **Compacted topics and offset gaps are supported.** Parallel Consumer never assumes offsets are
> contiguous - it tracks completion for the records it was actually given, so a hole in the log, from
> compaction, a transaction marker or an aborted transaction, is simply not a record it is waiting
> for. There is no range to reconcile and no gap to special-case.

The order matters: *"we support compacted topics"* is what someone searches for; *"we never assume
contiguity"* is why they should believe it. Leading with the mechanism is a common failure in this
project's writing - it flatters the reader's patience and loses the ones who are scanning.

That is quotable, it is true, and it comes straight from `PartitionState`'s own javadoc: *"a gap in
the offsets is effectively the same as an offset which has succeeded - because either way we have no
action to take."* It is a **design property**, not a patch, which is a stronger claim than a list of
handled cases.

Supporting detail available if a reader wants it: compaction recovery (`confluentinc#409`, a feature
record, and a real-broker integration test that compacts mid-run), transaction-marker handling
(`confluentinc#329`, with `TransactionMarkersTest`), and aborted transactions needing nothing because
the client filters them before PC sees them.

**The audit this implies.** Offset gaps will not be the only one. Worth a pass over
`docs/features/*.yaml` asking of each: *is this stated anywhere a prospective user reads, or only in a
data file?* Candidates already visible - head-of-line avoidance, fair shard traversal, the retry
model, and the offset map itself, which is the biggest differentiator and the least explained.

**Per the rule above, none of this is written as "unlike others".** It is written as what PC does.

## Vocabulary: it is the landing page, not "the marketing page"

**Owner's direction, 2026-08-21.** The front page of this site is the **landing page**. Use that term
in notes, issues and commit messages. "Marketing page" is the wrong frame - it suggests copy written
to persuade, when what actually persuades this audience is a specific, checkable claim about their own
workload.

## Two things that go on the landing page, high up

Both come out of the analysis in [`market-analysis-llingr.md`](market-analysis-llingr.md) and
[`perf-throughput-regression-since-0-3.md`](perf-throughput-regression-since-0-3.md). Neither is a
comparison, and neither names anyone - per the rule above, both are written purely as what PC does.

### 1. The raw replay count, not the percentage

**Owner's direction, 2026-08-21: "the percent hides the real impact".** This is the single most
important framing correction to come out of the divergence benchmark.

The measured result: after one crash with a single stuck record in a 200,000-record partition, a
design that can only commit a contiguous prefix reprocesses **199,998 records**. PC reprocessed
**6,412**.

*"100% redelivered"* and *"6.4% redelivered"* are the same facts and they land as an abstraction.
**"199,998 records reprocessed after one ordinary rebalance"** is a number an operator can convert
into their own minutes, their own database load and their own on-call page. Percentages invite the
reader to reason about ratios; raw counts invite them to reason about consequences, and the
consequence is the point.

So: **the raw count is the headline, the percentage is the footnote** - and this rule applies wherever
the offset-map story is told, not only on the landing page. Show the record count first, the
proportion second, and state the record count in the units the reader already thinks in.

Worth pairing with the other measured number from the same run, because it makes the mechanism
concrete: PC's offset payload for that partition was **9 bytes** - a run-length encoding of
`[1, 199998]`, one incomplete record followed by one long run of completed ones. The entire recovery
state for 200,000 records fits in a `long`'s worth of commit metadata.

### 2. The admission-stall scenario

**A scenario, not a claim** - the reader checks it against their own key distribution rather than
taking a number on trust. It is the clearest available illustration of why PC dispatches from shards
rather than from a fixed pool of key slots.

> **The workload.** A partition contains 1 record with key `A`, then 249 records with key `B`, then
> 1,000 records each with a distinct key. Key `B` is slow - it calls something that takes a while.
>
> **What PC does.** All 1,251 records are placed into their shards on arrival. The control loop
> dispatches across every shard up to the in-flight target, so the 1,000 distinct keys are processed
> immediately and concurrently, while `B`'s 249 records proceed in order on their own shard. The slow
> key delays exactly the records that share its key - and nothing else. No record waits for admission,
> and the partition's throughput is not set by its slowest key.

The property being demonstrated has a name worth using on the page: **work is admitted by shard, not
by slot.** A design that admits records into a bounded set of key slots before it knows the key can
have every slot held by one slow key while idle capacity waits behind it. PC has no admission stage to
block at - a record's arrival costs a shard insert, and dispatch decides concurrency afterwards, with
full knowledge of what is queued.

This is also the honest limit to state alongside it, per the volunteer-the-boundary craft note above:
**records sharing a key are still serial by design** - that is the ordering guarantee, not a
deficiency - and `B`'s own 249 records do take as long as they take. What PC guarantees is that the
other 1,001 records do not wait for them.

Renders well as an animation: 1,251 tiles, one column per key, filling in parallel while the `B`
column drains slowly - the visual makes the point faster than the prose does.

## The build brief - P0 is content, and it now exists

**Owner's direction, 2026-08-21:** *aim for something as good as the best site in this space; at least
P0 is content-wise what do we want to show; making it look nice is a future stage; make a structured
file to lay out the content so an independent agent can go.*

**That file is [`docs/data/landing-page.yaml`](../data/landing-page.yaml).** It holds the running
order, the copy that is ready, the evidence behind every claim, and an explicit `status` on each
section saying whether it can publish. It is deliberately structured data rather than prose so the
brief is unambiguous and the builder does not have to re-derive the argument from these notes.

**Fourteen sections, in order:** hero · the problem · the offset map · admission by shard · ordering
modes · correctness · reach · benchmarks · try it · licence · provenance · ecosystem · roadmap ·
quickstart.

Three of them are **blocked on evidence rather than on design**, and that is the useful output of
writing it down:

- **`reach`** cannot publish until a Kafka client/broker version support matrix exists. There is no
  feature record for it. A vague compatibility claim is worse than none, because the reader's next
  question is the exact version they are on.
- **`benchmarks`** needs the key-distribution sweep from
  [`next-performance-regression-testing.md`](next-performance-regression-testing.md). Publishing one
  number from one distribution is the thing this note criticises elsewhere.
- **`provenance`** needs a public, dated record of defects closed and guards added. Credibility is not
  won by claiming it, and it is the constraint that matters most for this project.

## How the best site in this space is actually built - and why that is good news

**Inspected 2026-08-21**, because "it looks expensive" and "it is expensive" are different claims and
only one of them is a reason not to try.

It is **static HTML with a single stylesheet, produced without a site framework.** No Next, Astro,
Nuxt, Svelte, Hugo, Jekyll, Webflow or Framer fingerprint anywhere in the markup.

**Correction, and it matters: "hand-authored" was an over-claim.** What the markup actually shows is
the absence of a *generator*, which is not evidence that a person typed it. Static HTML and CSS of
this quality is well within what a coding agent produces, and given the project's scale - one
maintainer, a Go engine, a Rust crate, a documentation site and this - **agent-generated is the more
likely explanation than hand-typing.** The observable fact is "no framework"; authorship is unknown. One `styles.css`. Two
self-hosted subset `woff2` fonts, one for text and one for code. Roughly 13KB of hand-written vanilla
JavaScript for the interactive pieces. JSON-LD structured data. Static hosting behind a CDN. **No
analytics and no third-party requests at all.**

The stylesheet opens with a design-token palette - two colour scales of about a dozen steps each,
declared once as custom properties and used throughout.

**The lesson, and it survives the correction - in fact the correction strengthens it.** There is no
framework to choose, no build pipeline to maintain, and nothing that needs upgrading. Two decisions
carry most of the result - a token palette and a type scale - and everything else follows from them.

**If it is agent-generated, that is the finding, not a deflation of it.** It means a page of this
quality is reachable here, now, for the cost of specifying it well - which is exactly why the P0
deliverable is a content file with the argument and the evidence settled, rather than a design brief.
**The scarce input is knowing what to say and being able to prove it, and that is the part no agent
can supply for us.** The palette, the type scale and the markup are the cheap half.

**What that means for our P1:**

- **Do not reach for a site framework.** This is a handful of hand-written documents.
- **Palette and type scale first.** Make those two decisions properly and the rest is assembly.
- **Self-host and subset the fonts.** No third-party requests: faster, and a privacy position worth
  having next to the licence position.
- **Spend the entire design budget on the two animations** - the offset map and shard dispatch. They
  are the things a paragraph cannot do, and they carry the two strongest claims on the page.
- **No analytics.** Costs nothing to omit and is consistent with everything else the page says.
