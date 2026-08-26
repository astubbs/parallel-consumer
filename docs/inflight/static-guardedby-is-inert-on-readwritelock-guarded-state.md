# `@GuardedBy` silently checks nothing on ReadWriteLock-guarded state

<!-- inflight-type: register -->
<!-- inflight-labels: concurrency -->
<!-- inflight-impact: misdirection -->

`@GuardedBy` is now writable in main code - `error_prone_annotations` is declared at `provided`
scope in the root pom, and Error Prone's check fires at ERROR on a plain monitor. **On a
`ReadWriteLock` it does not.** Both spellings are wrong, in opposite directions, and the one a
reasonable person writes first is the one that reads as a passing check.

The engine's `AGENTS.md` ("Annotate a race with `@GuardedBy`") owns the obligation and routes here;
this note owns the measurement.

## Measured, with a control arm

Identical unguarded read; only the lock type and the spelling vary.

| Spelling | Result |
|---|---|
| `@GuardedBy("aPlainObject")` | fires at ERROR, names the lock and the access. The check works. |
| `@GuardedBy("rwLock")` where `rwLock` is a `ReentrantReadWriteLock` field | **accepted, then not analysed. Nothing is reported.** |
| `@GuardedBy("rwLock.readLock()")` | fires - but also on every access legitimately holding the *write* lock: `instead found: 'this.lock.writeLock()'` |

`@GuardedBy` carries one lock expression, and a ReadWriteLock is two. So the annotation cannot
express "read under the read lock, write under the write lock" at all. **Do not annotate
ReadWriteLock-guarded state**; in this engine that means `RetryQueue` and `ProducerManager`.

The inert case is the dangerous one: `@GuardedBy("lock")` on `RetryQueue`'s `unique` and `sorted`
compiles clean, looks like an enforced invariant to every future reader, and would have let the
real defects below through untouched.

## What the enforcing spelling found in `RetryQueue`, site by site

Run once with `@GuardedBy("lock.readLock()")` on `unique` and `sorted`, then reverted - the
annotations are not in the tree, because neither spelling is correct. Line numbers are the
unannotated file.

| Site | Verdict |
|---|---|
| `size()` -> `unique.size()`; `isEmpty()` -> `unique.isEmpty()` | **Real, but dead.** Zero callers in main or test. |
| **`removeAll` - `if (toRemove == null \|\| unique.isEmpty())`, before the write lock is taken** | **Real and live.** The one that matters. |
| `iterator()` -> `sorted.values().iterator()` | False positive - the read lock is taken on the line above and released by the iterator's `close()`. |
| `getNumberOfFailedWorkReadyToBeRetried` -> `sorted.isEmpty()` / `sorted.lastEntry()` | False positive - called from inside the caller's read lock. |

**Independent corroboration, which is why this is not a scanner curiosity:**
`docs/inflight/static-infer-findings.md` already records `this.unique` read via
`Map.size()`/`isEmpty()` racing with writes, from a completely different engine. Two analysers, one
defect.

## The live one is FIXED; what remains is the annotation limitation

`removeAll`'s off-lock `unique.isEmpty()` fast path is gone, and `size()`/`isEmpty()` now take the
read lock. Deleted rather than moved inside the lock: the loop is already idempotent per key, so the
guard only ever saved one uncontended lock acquisition on an empty queue while costing a JMM
violation on a work-selection path.

Three identities retired from `config/infer-known-findings.txt` - `RetryQueue.size`, `.isEmpty`,
`.removeAll`. The four `RetryQueueIterator` entries remain: they are the separate `closed` field
defect, named in `docs/refactoring.md`.

**What is NOT claimed, then or now:** that the stale entry reached duplicate delivery.
`getNumberOfFailedWorkReadyToBeRetried` counts work "ready to be retried but not inflight yet", so
there is in-flight awareness downstream that may absorb a re-issue. The race and the no-op were
established; the blast radius past that point was not, and the fix does not depend on it.

**Why this note survives its own bug fix:** the `ReadWriteLock` limitation above is unchanged, so
`RetryQueue` and `ProducerManager` still cannot carry `@GuardedBy` at all - which means the race just
fixed has nothing preventing its return. That is precisely the gap the annotation exists to close and
cannot close here.

## Delete when

The `ReadWriteLock` limitation is either fixed upstream in Error Prone or recorded somewhere more
durable than this note. The race half is already closed. Until then this is a register: consulted
before anyone writes an annotation on a lock-heavy class, not a task anyone completes.
