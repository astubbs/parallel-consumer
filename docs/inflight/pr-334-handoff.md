# astubbs/parallel-consumer#334 - session record, and what this session negated

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

Written 2026-08-25 at head `a8d51e1b2`, for a move to another machine. **Delete when astubbs#334
merges.**

**An earlier handoff was deleted this same day** for restating facts other notes own, and that
reasoning still holds - so this one deliberately carries *only what nothing else owns*: the review
arc, the claims that were asserted and then negated, and which of them came from me rather than from
a document. Everything about the work itself is a pointer.

## Read these first, in this order

| Document | Why it comes here in the order |
|---|---|
| [`streams-coupling-dimensions.md`](streams-coupling-dimensions.md) | The register of what to prove and what it would falsify. Start here; it frames everything else. |
| [`streams-verify-against-the-kafka-sources.md`](streams-verify-against-the-kafka-sources.md) | **Read before writing any claim about Kafka behaviour.** Written this session because reasoning about this framework produced confident wrong answers three separate times. |
| [`../plans/2026-08-25-001-feat-streams-windowed-aggregation-plan.md`](../plans/2026-08-25-001-feat-streams-windowed-aggregation-plan.md) | The live artifact - the windowing falsification spike, at its third draft. |
| [`test-cross-binding-streams-conformance.md`](test-cross-binding-streams-conformance.md) | How to test N bindings without writing the suite N times; earmarked as a product feature. |
| [`../solutions/architecture-patterns/one-answer-slot-for-many-callers-is-a-correlation-bug.md`](../solutions/architecture-patterns/one-answer-slot-for-many-callers-is-a-correlation-bug.md) | The query-correlation bug fixed this session, and the ordering trap in fixing it. |
| [`pr-blockers-and-collisions.md`](pr-blockers-and-collisions.md) | Why astubbs#334's dependency check is red **by design**, and why the Codex commit was cherry-picked rather than merged. |
| [`ci-standing-citation-and-tag-debt.md`](ci-standing-citation-and-tag-debt.md) | Why two gates are red on debt rather than on anyone's change. |

## What shipped this session

Working code: the KStream-KTable **join** (proved end to end against a real broker - map, join and
reduce in one topology, three Python functions); **query correlation**, closing a silent bug where
two concurrent host threads received each other's answers; and **registered functions moved off the
reader thread**, so a host function may now call back into the engine while being invoked.

Everything after that is documents. The windowing spike is **planned and reviewed, not started** -
no measurement has run, no windowing code exists.

## The claims this session asserted and then negated

Recorded because a fresh agent reading the conversation, the commits or an early draft will meet the
superseded version. Each is settled; none is an open question.

**About the product:**

- **Host-supplied serdes as the strongest remaining falsifier** - proposed, then withdrawn the same
  day. The engine uses `Serdes.ByteArray()` everywhere except where an operator mints a value it
  created; the host serialises in its own language, so nothing ever hands the engine a host serde to
  call. Held in the register so the next person to have the idea finds the refutation.
- **Punctuators as the cheap next check** - the deferred-capability table calls them cheap. Rejected:
  a punctuator that cannot `forward()` is close to useless, and `forward()` needs one-in-many-out,
  still open. They are blocked behind it, not cheap.
- **Re-entrancy would falsify the approach** - predicted, and the *hang* was confirmed exactly. The
  *inference* was wrong: the wire was already multiplexed and the deadlock was one design decision in
  one Python file. Both halves are kept in the register on purpose.
- **"P2 sidesteps the window multiplier"** - the second draft's organising idea, refuted against the
  3.9.2 sources. A windowed store caches per *windowed* key, so twelve overlapping windows are twelve
  cache entries and twelve emits per flush. P2 divides P1's count by records-per-key-per-flush and by
  nothing else.
- **F2, the bet-off floor, at 100 records/second** - withdrawn. Its own argument does not reach it,
  and the defect was subtler than "wrong number": the argument omits that a host reimplementation
  must also *consume from Kafka single-threaded*, which is where its real ceiling sits. F2 is now a
  measured arm, with the *rule* pre-registered rather than the number.
- **The 8,300 crossings/second ceiling** - withdrawn. The lock guards each outbound message, not the
  whole crossing, which is why the measured 9,501 exceeds a supposedly whole-JVM limit.

**Claims I made that were wrong, listed because they are in the conversation and may be believed:**

- I quoted `STRATEGY.md` as saying *"feature parity is the goal, not speed - it does not need to beat
  JVM Kafka Streams; it needs to exist."* **That sentence did not exist.** I relayed it from an
  agent's report without checking, then repeated it and passed it into a subagent brief. The owner's
  judgement was that it *should* exist, so it is now genuinely in `STRATEGY.md`'s Kafka Streams
  section - but it arrived by that route, not by quotation.
- I said `TopologyTestDriver` **disables caching** and that a suite recorded against it
  **under-counts**. Both wrong: it builds a cache and commits per record, which flushes it, so it
  emits every intermediate update and therefore **over-counts** versus a broker run. That error
  reached the plan through my brief before being corrected.
- I added a Marginal verdict branch and introduced two contradictions doing it - a stop-condition
  count that still said "three", and a Goal Capsule that said nothing stops where U6 said hopping
  stops. Fixed in `25e209e68`.
- I praised F2's derivation as the best thing in the plan. Its arithmetic did not hold.

## How the review actually went, because the method transferred better than the findings

Three rounds against the plan, and **each defect class was caught by a different mechanism** - worth
knowing before choosing how to review the next draft.

1. **A cross-model pass (Fable), asked one adversarial question** - *can this plan come out negative,
   or is it built to confirm?* It found every stated falsifier pointed at the optimistic outcome. That
   drove a full rewrite. No persona reviewer found it.
2. **Persona reviewers, reasoning about the document** - found the verdict machinery: a pool that let
   a tumbling result rescue a hopping failure, an inert Marginal branch, arms differing in two terms.
3. **The reviewer that read Kafka's sources** found what none of the others could, twice, including
   the refutation of the second draft's central premise - and corrected two reasoning reviewers on
   matters of fact. That is what
   [`streams-verify-against-the-kafka-sources.md`](streams-verify-against-the-kafka-sources.md)
   exists to institutionalise.

**No reviewer has seen the third draft (`a8d51e1b2`).** A fourth round is available and unrun.

## State, and what is fragile

- Branch `research/kafka-streams-foreign-wrappers`, clean, level with origin, **five commits behind
  `origin/master`**. Both sides are package-renamed, so a catch-up merge is ordinary.
- **Sibling branch `fix/node-gates-preflight`** is pushed and has **no PR**. It fixes gate scripts that
  reported "policy violated" when node could not start; astubbs#341 carries a comment explaining it,
  and its agent was told to wait for it.
- **A stale `NODE_OPTIONS` preload breaks every `node` invocation on the capture machine**, which
  makes the citation and issue-ref gates fail as if the policy were violated. Workaround used
  throughout: prefix with `env -u NODE_OPTIONS`. Machine-local; may not follow the move.
- Scratchpad artifacts from this session are machine-local and **will not survive** - everything that
  mattered is in the notes above or in commit messages.

## Explicit direction from the owner, kept separate from the status above

- The work is moving to a machine with **Fable credits available**. If a fourth review round runs, the
  unrun question is whether the third draft's floors and branch structure cover the outcomes its own
  predictions make likely - not a repeat of round one's question, which is answered.
- **The commit history is deliberately not re-cut.** The owner's reasoning: on this branch the trail
  of plan → review → refutation → rewrite is part of the deliverable, and re-cutting is a merge-prep
  step, not a mid-flight one. `b19e0be50` carries three concerns under one message as a result; that
  is known and accepted.
