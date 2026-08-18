# Dead letter queue (DLQ): prior-art report

**Status:** brainstorm in progress. This document is the research record; the requirements plan will
follow it in a separate dated document on the same branch.
**Written:** 2026-08-18 (research run 2026-08-17)
**Tracking:** astubbs#149 (mirror of confluentinc#310)
**Deliberate decision:** astubbs#8 (`features/retry-dlq`, 2022) stays open, referenced from here, and
is intended to be *consumed as the implementation PR* once the requirements settle - overriding the
"close or finish" instruction in `docs/inflight/pr-blockers-and-collisions.md`, which this branch
updates in the same commit.

## Prior-art checks (all six, with empty results stated)

| Check | Result |
|---|---|
| `docs/plans/` | Nothing on DLQ. |
| `docs/solutions/` | Nothing on DLQ. |
| `docs/inflight/` | `next-candidates.md` ranks DLQ "the most-demanded missing feature. Large, and spec-stage only."; `release-0.6.0.0.md` excludes it deliberately; `next-groom-1-0-release-train-issue.md` names it for release-train grooming; `bug-poisoned-transaction-not-aborted-while-running.md` wants its abort decision settled alongside retry/DLQ work; `bug-max-failure-history-is-inert.md` is in the same design space. |
| Open fork PRs | astubbs#8 "START: DLQ draft" - the only DLQ code in existence (see lineage below). |
| Merged fork PRs | Nothing DLQ-related. |
| Issues, `--state all`, both repos | The demand ledger below. |

Additionally, every branch tip in both repositories (248 refs, full fetch) was grepped for
`dlq|dead.?letter` in Java sources. Real DLQ implementation exists only on
`origin/features/retry-dlq`. The `TerminalFailureReaction` enum (with a DLQ placeholder) appears on
`features/retry-exception`, `features/retry-exception-w-terminal` and
`features/partial-batch-failure`; an example router sits on `parallel-webservice`. Every other hit
is the long-deleted no-op test stub `poisonPillGoesToDeadLetterQueue` (its autopsy:
`docs/refactoring.md`, and the audit in `docs/test-hardening/`).

## Demand ledger (fork mirrors in parentheses)

Direct asks:

- **DLQ implementation** - confluentinc#310 (astubbs#149), labels `pr-available` +
  `next-feature-release`. Its own body records the community workaround: produce to your own DLQ
  topic, then swallow the exception so PC marks the record succeeded.
- **Max retries + exhaustion callback** - confluentinc#196 (astubbs#141), same labels. "When the
  count is reached, a callback should be executed ... maybe produce it to a different topic (DLQ)."
- **Enhanced retry epic** - confluentinc#65 (astubbs#239). Closed upstream "completed" with exactly
  these two children unfinished.
- **Stall monitor / shutdown / skip / DLQ** - confluentinc#34 (astubbs#231).
- **Scheduled retry** - confluentinc#48 (astubbs#234).

Adjacent demand, same nerve ("what happens when a record cannot succeed"):

- Terminate processing - confluentinc#718 (astubbs#172, `next-breaking-release`).
- "Is there any exception handler?" - confluentinc#550 (astubbs#163, `next-breaking-release`).
- Serialization / deserialization error handling - confluentinc#391 (astubbs#153), confluentinc#304
  (astubbs#148).
- One bad record fails the whole batch - confluentinc#887 (astubbs#189).
- Stop retrying after N failures - confluentinc#248 (closed as question).
- Circuit breaker - confluentinc#110 (closed; README documents the DIY composition under its
  "Circuit Breaker Pattern" heading).
- Users were still commenting on the closed terminal-exception PR (confluentinc#291) in 2024-2025
  asking for the `SHUTDOWN` reaction to ship.

The repo's own editorial record agrees: `src/docs/development/upstream-pr-analysis.adoc` calls
confluentinc#310 the single most impactful missing feature and its verdict is to revive
confluentinc#366 and confluentinc#291 as a pair. The roadmap entry (`docs/data/roadmap.yaml`, id
`dead-letter-queue`) sets horizon `next-0x`, `blocks_1_0: false`, done-when "a record that exhausts
its retries can be diverted rather than blocking or being dropped silently".

## The one implementation lineage (2022, never landed anywhere)

- **confluentinc#291** (closed unmerged) introduced `PCTerminalException`, `PCUserException`,
  `TerminalFailureReaction` (`SHUTDOWN`/`SKIP`) and the `UserFunctionRunner` extraction. Only
  `PCRetriableException` ever reached master.
- **confluentinc#366** (closed) = **astubbs#8** (open draft, branch `features/retry-dlq`): adds
  `TerminalFailureReaction.DLQ`. On a terminal failure it builds `ProducerRecord`s carrying `pc-`
  provenance headers (`pc-failure-count`, `pc-last-failure-at`, `pc-last-failure-cause`,
  `pc-partition`, `pc-offset`), produces them synchronously through `ProducerManager`, auto-creates
  the target topic via a new `AdminClient` option, then marks the record succeeded so the offset
  advances.
- Draft-stage defects observed in the astubbs#8 diff, for whoever revives it:
  1. Records are produced back to the **source topic** - `prepareDlqMsgs` uses
     `recordContext.topic()` and the target is never rewritten to the DLQ topic.
  2. `DLQ_SUFFIX` (".DLQ") is *prepended* in `tryEnsureTopicExists` (`DLQ_SUFFIX + name`), so even
     topic creation and the intended naming disagree.
  3. Topic auto-creation triggers on catching `TopicExistsException` - inverted; the retry-then-create
     path should react to an unknown-topic error.

## What master does today when a record fails

- There is no terminal state. `WorkContainer.onUserFunctionFailure` (see `updateFailureHistory`)
  increments `numberOfFailedAttempts`, records `lastFailureReason`, computes `retryDueAt`, and the
  record becomes available again via `isAvailableToTakeAsWork`. The README states it plainly under
  its `skipping-records` anchor: "messages will continue to be retried forever", blocking progress in
  `PARTITION` ordering.
- Machinery a DLQ would lean on already exists: `retryDelayProvider`, `PCRetriableException`,
  `RecordContext.getNumberOfFailedAttempts()` / `getLastFailureReason()`, and the producer path
  (`ProducerManager.produceMessages`, the `pollAndProduce*` entry points - note their output types
  are pinned to the input `K, V`).
- The README's Share Groups comparison table ("Poison messages" row) already positions broker-side
  archiving (`group.share.delivery.attempt.limit`) against PC's DIY status quo - a DLQ changes that
  row.

## Decisions already queued to be settled with this work

- `src/docs/development/upstream-map.yaml`, entry `sweep-2023-retry-lifecycle`: decide the
  retry-delay precedence (exception > provider > default) once across confluentinc#196,
  confluentinc#310, confluentinc#48 and confluentinc#82; `PCRetriableException` carries no
  `Duration`.
- `docs/inflight/bug-poisoned-transaction-not-aborted-while-running.md`: "a DLQ or a max-attempt
  terminal outcome would give this defect somewhere to go".
- `docs/inflight/bug-max-failure-history-is-inert.md`: `maxFailureHistory` is settable and read
  nowhere - implement or delete.

## Open questions the brainstorm must answer

1. Core abstraction: is the 2022 shape (terminal exception -> per-consumer reaction enum -> PC-owned
   DLQ produce) still right, or has the retry-lifecycle sweep / Share Groups / 1.0 API settlement
   changed the picture?
2. What triggers "terminal": only an explicit user-thrown terminal exception, a configured max-retry
   count, either, or a pluggable policy?
3. Who owns the DLQ produce: PC (topic naming, headers, auto-creation, transactions) or a user
   callback handed the exhausted record?
4. Transactions: what does DLQ mean under `PERIODIC_TRANSACTIONAL_PRODUCER` - same transaction as
   the offset commit?
5. Failure semantics of the DLQ produce itself: block, retry, or shut down when the DLQ is
   unavailable?
6. Scope boundary against the adjacent demand: which of max-retries (confluentinc#196), terminal
   exceptions (confluentinc#291's ghost), skip/shutdown reactions, and deserialization failures
   (confluentinc#304) ride along, and which stay out?
