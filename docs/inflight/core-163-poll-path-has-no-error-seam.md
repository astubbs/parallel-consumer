# No poll-path exception handler: the answer to astubbs#163, and the premise it corrects

<!-- inflight-type: feature -->
<!-- inflight-impact: crash -->
<!-- inflight-vetted: 2026-09-07 - every claim re-checked against the tree: `ConsumerManager` still catches only `SaslAuthenticationException` (bounded retry) and `WakeupException` (`correctPollWakeups++`), `BrokerPollSystem#supervise` still wraps the death in `PCInternalRuntimeException` and its "Unknown error" catch still calls `notifyPollerDied` before rethrowing, and `maybeCloseConsumer` is still gated on `isResponsibleForCommits()` (`committer instanceof ProducerManager`) so consumer-commit modes send no LeaveGroup. `gh issue view` says 163/148/153/149/141 are all still OPEN and 163 has no comments, so the draft answer is still unposted. Removed the delete-when section per docs/inflight/AGENTS.md; its condition (answer posted, policy decided) is unmet -->

[astubbs#163](https://github.com/astubbs/parallel-consumer/issues/163) (confluentinc#550) asks whether
PC has an exception handler, and then asks the question that was never answered: *"is there a plan to
add error handler processing, or do we need to customize it?"*

Below is a draft reply, written to be postable as-is. Under it are the two things the reply depends on
that no command can tell you, one of which is currently wrong in live requirements work.

## Draft answer (not yet posted)

> No, there is no handler for a deserialization failure thrown out of `consumer.poll()`, and that
> is still true today. The poll path does catch two specific exceptions - a bounded
> `SaslAuthenticationException` retry, and `WakeupException` as control flow - but nothing catches
> yours. You need to customize it, and there is a plan, but no date. Here is the whole picture.
>
> **Your stack trace is the poll path, not the processing path, and that distinction is the answer.**
> PC's documented error handling - retry with a delay, skip by returning normally - all lives on the
> processing path and operates on a record PC has already deserialized and queued. A
> `RecordDeserializationException` is thrown by `consumer.poll()` before any of that exists, so none of
> it can apply.
>
> **What happens today:** the exception propagates out of the broker-poll thread, which dies. PC then
> closes the whole instance and stores the cause, reachable through `getFailureCause()` after the fact.
> So a single undeserializable record stops consumption for that instance, and the cause arrives
> wrapped in generic exception types rather than something you can usefully switch on.
>
> **The workaround that works today**, and the one we would recommend: consume as `byte[]/byte[]`,
> deserialize inside your processing function, and handle the failure there. That moves the failure
> onto the processing path, where you can log and skip it, divert it to a topic of your own, or rethrow
> to retry it.
>
> **One thing to know before you rely on that:** on the processing path every exception is treated as
> retriable and a record is retried indefinitely - there is no max-retry ceiling and no terminal state.
> "Give up on this record" means catching it yourself and returning normally, which PC records as a
> success so the offset advances. There is no built-in dead-letter queue yet.
>
> **The plan.** Two issues track this, and they are deliberately different sizes:
> [astubbs/parallel-consumer#148](https://github.com/astubbs/parallel-consumer/issues/148)
> (confluentinc/parallel-consumer#304) is the contained step - stop a bad record from killing the poll
> thread - and
> [astubbs/parallel-consumer#153](https://github.com/astubbs/parallel-consumer/issues/153)
> (confluentinc/parallel-consumer#391) is the full handler/policy API, along the lines of the Kafka
> Streams handler suggested on the original thread. The dead-letter-queue and max-retries work
> ([astubbs/parallel-consumer#149](https://github.com/astubbs/parallel-consumer/issues/149),
> [astubbs/parallel-consumer#141](https://github.com/astubbs/parallel-consumer/issues/141)) is a
> separate track and would *not* cover your case: a record that fails to deserialize never enters the
> retry system at all, so nothing that triggers on retry exhaustion can ever fire on it.
>
> Note that the Confluent-hosted repository is no longer maintained. This fork is where the work
> happens, so discussion belongs here.

## What the answer depends on that is not in the issue

**The poll is not unguarded, and the issue body's claim that it is has gone stale.** The body quotes a
blanket `catch (Exception e)` as the only handling. In fact `internal/ConsumerManager.java` has
`catch (SaslAuthenticationException`, which retries against a bounded budget, and a `WakeupException`
arm that returns an empty result (grep `correctPollWakeups++`; `WakeupException` itself is not unique
in that file). A **typed per-exception poll-error seam already exists and is load-bearing** -
a deserialization policy would be a third arm of it rather than new architecture. That makes
astubbs#148 materially cheaper than the mirror bodies suggest, and nothing else records it.

**The quoted catch block has also changed.** astubbs#204 added
`committer.ifPresent(c -> c.notifyPollerDied(e))` to the `log.error("Unknown error", e)` catch in
`internal/BrokerPollSystem.java`, before the rethrow,
so a control thread blocked in `ConsumerOffsetCommitter#commitAndWait` is released by an event instead
of waiting out `offsetCommitTimeout`. The thread still dies and PC still closes; only the reporting
improved.

**"PC then closes the whole instance" now includes the Kafka consumer, in every commit mode - it did
not until September 2026.** `maybeCloseConsumer` in `internal/AbstractParallelEoSStreamProcessor.java`
was gated on the transactional committer alone, so a poller death in the consumer-commit modes sent no
LeaveGroup and the member's partitions stayed assigned to it until `max.poll.interval.ms` expired. It
has a second arm for a poll thread that ended without closing the consumer:
`docs/solutions/logic-errors/a-duty-assigned-by-role-is-unassigned-when-the-role-holder-dies-2026-09-08.md`.
That removes the group-level consequence, and leaves this note's subject untouched - **the poll thread
still dies, and PC still terminates.** A seam that let the poll loop *continue* is what would change
that; one that terminates the instance would not.

**The death is wrapped twice** - `void supervise()` in `internal/BrokerPollSystem.java` into
`PCInternalRuntimeException`, then `failureReason = new RuntimeException` in
`internal/AbstractParallelEoSStreamProcessor.java` into a bare one. That is the complaint in
`core-exception-hierarchy-cleanup.md` reaching a user-facing path, and it is why the draft says the
cause is not something you can switch on.

## The collision, and it is a wrong premise in live work

The DLQ prior-art report on astubbs#313 files astubbs#163 under "adjacent demand, same nerve", and its
**open question 6** asks whether deserialization failures ride along with the DLQ work. **On the
mechanism they cannot.** A record that fails to deserialize never becomes a `WorkContainer`, so
`numberOfFailedAttempts` never increments and no terminal-exception, max-retry or DLQ trigger can ever
fire on it. Answering question 6 "rides along" would settle requirements on a false premise.

For the same reason `bug-max-failure-history-is-inert.md` is unrelated here, being per-`WorkContainer`
too, and `OffsetCommitBudgetExceededException` (astubbs#204) does not collide either, being the commit
path.

What genuinely should be shared is the *shape* of the answer. astubbs#317
(`core-commit-failure-seam.md`) is the same request on a different path: hand the application a
decision instead of PC terminating for it. Whatever decision type astubbs#317 settles on should be
reused here rather than invented twice.

## The decision only the maintainer can make

Whether PC's contract is "an undeserializable record kills the instance" - which is Kafka's own
consumer behaviour, and defensible - or whether PC offers skip / divert / shut-down like Kafka Streams'
handler. That choice, not the code, is what has kept astubbs#148 and astubbs#153 open for years. The
cheap half is independent of it: the `byte[]` route is **absent from the README**, whose Retries and
Skipping Records sections describe only the processing path.

Recommended disposition for astubbs#163 itself: post the answer, correct the stale poll-is-unguarded
claim in the body, then close it as a duplicate of astubbs#153 with astubbs#148 as the contained step.
Answering before closing matters - the reporter asked in 2023 and has never had a fork answer.
