# PC's exceptions have three roots and no consistent name

<!-- inflight-type: task -->
<!-- inflight-impact: refactor -->
<!-- inflight-state: deferred - after v6, structural work that should not ride a release -->


**Priority: high, and easy.** Mechanical, well-bounded, and the major-version gate is open
(`docs/refactoring.md` - `0.6.0.0` is the major being cut and already carries a `=== Breaking`
section), so it can land now rather than waiting for a bump.

## The cost, observed: a generic catch hides which routes are reachable

<!-- post-merge: checked-begin -->
**Operator's diagnosis, 2026-08-27, and it is the sharpest argument in this note.** The mailbox
guards astubbs#267 added were written as `catch (Throwable)`. Asked what could actually make
`addToMailbox` throw, nobody could answer from the code - the catch names nothing, so the reachable
route has to be rediscovered by reading three call levels down. It turned out to be
`WorkContainer#onPostAddToMailBox` releasing the produce lock, where `ProducerManager`'s
`ensureProduceStarted` throws `PCInternalRuntimeException` if the hold count is below one - the same
invariant [`bug-producing-lock-double-release.md`](bug-producing-lock-double-release.md) has an open
question about.
<!-- post-merge: checked-end -->

**Had PC thrown, and caught, its own specific types, that would have been legible at the call site**:
a catch naming a produce-lock exception says what can happen and roughly why. `Throwable` says only
that the author was being careful. So the hierarchy work below is not only about giving users one
type to catch - it is about making PC's own guards state what they are guarding against.

The tension to resolve when doing it: these guards must still catch broadly, because anything
escaping them strands the sibling records behind it. The answer is to catch broadly and **classify**,
not to narrow the catch - `failFatallyOnUnmailboxableRecord` does that now, and reads much better for
it than the bare log line it replaced.

## What is wrong

Six exception types, three unrelated roots:

| Type | Extends |
|---|---|
| `ParallelConsumerException` | `RuntimeException` |
| `ExceptionInUserFunctionException` | `ParallelConsumerException` |
| `OffsetCommitBudgetExceededException` | `ParallelConsumerException` |
| `PCRetriableException` | **`RuntimeException`** |
| `PCInternalRuntimeException` | **`RuntimeException`** |
| `InternalException` | **`Exception`** |

**`catch (ParallelConsumerException)` does not catch what PC actually throws at you.**
`PCInternalRuntimeException` is what arrives from `getFailureCause()`, and it is the class in the
original upstream report: `...internal.PCInternalRuntimeException: Timeout waiting for commit response
PT30S` (confluentinc#833). A user wanting "catch everything this library throws" has no single type.

**The names are inconsistent too**, which is the part you notice first in a log or an IDE's exception
picker: `PCRetriableException` uses a prefix, `ParallelConsumerException` spells it out, and both
`Internal*` types mark nothing at all - `PCInternalRuntimeException` in a stack trace that prints simple
names could belong to anything. The fully-qualified form disambiguates; plenty of surfaces do not show
it.

**This is not a new opinion.** `ProducerManager` already carries
`// TODO(refactor): PCInternalRuntimeException misnames a failed send; throw a specific subclass and
rename \`exception\` to \`sendFailure\`` - the same complaint, reached independently, about a
different call site.

## What to do

1. **Give everything one root.** `PCInternalRuntimeException`, `PCRetriableException` and
   `InternalException` should descend from a PC type, so one `catch` covers the library. This is the
   half that changes behaviour for users, and the half worth doing first - the naming is cosmetic
   beside it. Still open.
2. **Then settle the naming convention and apply it once.**
   <!-- post-merge: checked-begin -->
   **Done for the `Internal*RuntimeException` leg, astubbs#267**: `InternalRuntimeException` was
   renamed to `PCInternalRuntimeException`, following the `PCRetriableException` precedent, so the
   unprefixed name no longer reads like a JDK type.
   <!-- post-merge: checked-end -->
   `OffsetCommitBudgetExceededException` still fails the convention it should have followed - it was
   added by astubbs#204 - and is the naming work still open here.

<!-- post-merge: checked-begin -->
The astubbs#267 rename was mechanical - a single identifier, source plus docs; `grep -rw
InternalRuntimeException` finds nothing left. What remains (the shared root, and
`OffsetCommitBudgetExceededException`'s naming) is smaller than what already shipped.
<!-- post-merge: checked-end -->

## Cheap and worth doing at the same time

`ConsumerOffsetCommitter` and `ConsumerManager` carry four `PCInternalRuntimeException` throws that
astubbs#204 added or reworked, three of which are user-facing failure paths (the poller-death report,
the abandoned-commit-on-close report, and the commit-response timeout). They are marked
`// TODO(refactor)` at the sites. They want a type that says PC, and probably a more specific one than
"internal runtime" for the two that a user is meant to read and act on.

## Delete this file when

The roots are unified and the naming convention is applied, or the idea is rejected with the reasoning
recorded in `docs/refactoring.md`.
