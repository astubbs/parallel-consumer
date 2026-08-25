# Next: exposing the Consumer API safely

<!-- inflight-type: feature -->
<!-- inflight-impact: coordination -->

Two related asks with very different amounts of prior art behind them. Searched exhaustively
2026-08-21; the split verdict is the point of this note.

## The Consumer API: substantial prior art, an implementation, and a 1.0 gate

**Tracking: [astubbs#158](https://github.com/astubbs/parallel-consumer/issues/158)**
(`confluentinc#520`), *"major: Safe User API exposure of ALL Consumer APIs (seek, end offsets etc)"* -
open, labelled `next-breaking-release`, **proposed for 0.7.0.0**. Upstream body: *"let's you use all
consumer APIs, including seek, end offsets, lag, assignment, paused, even poll, commit and pause"*.

The narrower asks that this would subsume: **astubbs#174** (`confluentinc#782`, seek to a specific
offset), **astubbs#246** (`confluentinc#191`, `seekToBeginning`), **astubbs#245**
(`confluentinc#187`, change subscription after start), and **astubbs#157** (`confluentinc#484`, *"Does
Parallel-consumer have state that we can read from?"*). Upstream Discussion 815, *"Getting/Setting
consumer state"*, is a real user asking for exactly this and has **zero replies**.

**The design problem, stated well in astubbs#158's fork-status section:**

> *"PC owns the consumer deliberately: it drives `poll()`, pausing, and commits, and its offset state
> (`PartitionState`, the incompletes encoding) assumes it is the only thing moving positions. That is
> why this is a 'major' - safely exposing `seek`, `endOffsets`, `pause` and friends means defining what
> happens to in-flight work and to the encoded incomplete-offset state when a user moves the position
> underneath it."*

**It gates 1.0.** `roadmap.yaml` entry `api-settlement` (horizon 1.0, `blocks_1_0: true`, tracking
astubbs#139) is done when *"the Consumer operations users need are exposed safely rather than by
reaching around the library"*. The thread-safety blocker **astubbs#139** (`confluentinc#186`) is the
gate on all of it, since `KafkaConsumer` is single-threaded and any user-facing call must be
marshalled onto the poll thread.

**An implementation already exists, unmerged.** `origin/features/consumer-interface` (upstream PR
confluentinc#346, closed unmerged) contains `FullConsumerFacade<K,V> extends Consumer<K,V>` with a
`setReactionMode(UnsupportedReaction)` switch to throw or log-and-swallow unsupported methods, plus
`ConsumerFacadeForPC` (interpret the intent and make the equivalent PC call - e.g. `pause(Collection)`
pauses all partitions and ignores the argument), strict and non-strict implementations, and three
integration tests. Commit messages include *"seek runs, works, need to handle truncation properly"*
and *"assignment() passes"*. `upstream-map.yaml` (`sweep-2023-consumer-api-exposure`) records it as
*"working and used in experimental tests"*.

**And a decision worth honouring:** `origin/features/producer-facade` ends at commit
`d7a118c0c end: doesn't make sense to have a producer facade`. The symmetric idea was tried and
rejected.

**What is exposed today: essentially nothing.** Only pause/resume/subscribe/close are public, and per
the audit below `pauseIfRunning`/`resumeIfPaused` are *"unsafe twice"* - a non-volatile read plus a
non-atomic check-then-set. `PCModule.consumer()` is a DI accessor, not a user API.

**Stranded artefact worth rescuing:** `docs/inflight/core-139-public-api-thread-safety-contract.md`
exists **only** on `origin/docs/triage-untriaged-mirror-issues`, and nothing on master points at it.
It carries a per-method audit of what a user may call off the constructing thread, with a verdict on
each, and the line that scopes this note: *"The full `confluentinc#346` ambition - exposing all
`Consumer` APIs safely - is a feature, and belongs with astubbs#158 rather than on a blocker."* Being
stranded on one unmerged docs branch is exactly the failure mode
[`next-fork-branch-archaeology.md`](next-fork-branch-archaeology.md) is about.

## The AdminClient is a separate question, and it is now a separate note

Searched at the same time and **no prior art exists** - the recollection of "an inflight to expose the
Admin-Client interface" is most likely this Consumer note, which does cover the seek/committed/position
surface. The AdminClient question is genuinely unasked and is scoped in
[`next-expose-admin-client-api.md`](next-expose-admin-client-api.md).

## Why this matters beyond user convenience

PC requires the user to **supply** the consumer, so a reference is always retained - unlike llingr's
franz adapter, where the client is unreachable unless a specific construction path was chosen up
front. **PC's users can already reach the client; what they lack is a defined contract for what is
safe to call while PC is running.** That reframes the work: it is less "expose the API" and more
"document and enforce the boundary that already exists but is unwritten".

## Related

- [`next-multi-topic-multi-function.md`](next-multi-topic-multi-function.md) - astubbs#245 (changing
  subscription at runtime) sits in both clusters.
- [`market-analysis-llingr.md`](market-analysis-llingr.md) - what llingr exposes, and the dead
  `BrokerQuery`.
- [`next-fork-branch-archaeology.md`](next-fork-branch-archaeology.md) - both the implementation
  branch and the design note are stranded on unmerged branches.
