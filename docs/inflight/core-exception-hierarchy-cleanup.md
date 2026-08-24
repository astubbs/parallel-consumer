# PC's exceptions have three roots and no consistent name

<!-- inflight-type: task -->
<!-- inflight-impact: refactor -->
<!-- inflight-state: deferred - after v6, structural work that should not ride a release -->


**Priority: high, and easy.** Mechanical, well-bounded, and the major-version gate is open
(`docs/refactoring.md` - `0.6.0.0` is the major being cut and already carries a `=== Breaking`
section), so it can land now rather than waiting for a bump.

## What is wrong

Six exception types, three unrelated roots:

| Type | Extends |
|---|---|
| `ParallelConsumerException` | `RuntimeException` |
| `ExceptionInUserFunctionException` | `ParallelConsumerException` |
| `OffsetCommitBudgetExceededException` | `ParallelConsumerException` |
| `PCRetriableException` | **`RuntimeException`** |
| `InternalRuntimeException` | **`RuntimeException`** |
| `InternalException` | **`Exception`** |

**`catch (ParallelConsumerException)` does not catch what PC actually throws at you.**
`InternalRuntimeException` is what arrives from `getFailureCause()`, and it is the class in the
original upstream report: `...internal.InternalRuntimeException: Timeout waiting for commit response
PT30S` (confluentinc#833). A user wanting "catch everything this library throws" has no single type.

**The names are inconsistent too**, which is the part you notice first in a log or an IDE's exception
picker: `PCRetriableException` uses a prefix, `ParallelConsumerException` spells it out, and both
`Internal*` types mark nothing at all - `InternalRuntimeException` in a stack trace that prints simple
names could belong to anything. The fully-qualified form disambiguates; plenty of surfaces do not show
it.

**This is not a new opinion.** `ProducerManager` already carries
`// TODO(refactor): InternalRuntimeException misnames a failed send; throw a specific subclass and
rename \`exception\` to \`sendFailure\`` - the same complaint, reached independently, about a
different call site.

## What to do

1. **Give everything one root.** `InternalRuntimeException`, `PCRetriableException` and
   `InternalException` should descend from a PC type, so one `catch` covers the library. This is the
   half that changes behaviour for users, and the half worth doing first - the naming is cosmetic
   beside it.
2. **Then settle the naming convention and apply it once.** `PCInternalRuntimeException` follows the
   `PCRetriableException` precedent. Whatever is chosen, apply it to
   `OffsetCommitBudgetExceededException` too - it was added by astubbs#204 and already fails the
   convention it should have followed.

Blast radius for the rename: **28 files, 63 occurrences**, almost entirely mechanical (24 main, 4
test). The root change is smaller than the rename.

## Cheap and worth doing at the same time

`ConsumerOffsetCommitter` and `ConsumerManager` carry four `InternalRuntimeException` throws that
astubbs#204 added or reworked, three of which are user-facing failure paths (the poller-death report,
the abandoned-commit-on-close report, and the commit-response timeout). They are marked
`// TODO(refactor)` at the sites. They want a type that says PC, and probably a more specific one than
"internal runtime" for the two that a user is meant to read and act on.

## Delete this file when

The roots are unified and the naming convention is applied, or the idea is rejected with the reasoning
recorded in `docs/refactoring.md`.
