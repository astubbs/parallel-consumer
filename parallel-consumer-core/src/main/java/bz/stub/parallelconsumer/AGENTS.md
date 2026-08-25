# Editing core main code: the concurrency rules that bind here

This is the engine. Two threads run through most of what follows - the **broker-poll thread** and the
**controller thread** - and the defects this project has spent the most time on are all the same
shape: state shared between them without a happens-before edge. What is below binds whenever you
change a field in this tree. Everything else about the project is in the repo-root `AGENTS.md`.

## Annotate a race with `@GuardedBy` in the same change that fixes it

**The build already enforces `@GuardedBy` and currently checks nothing**, because this codebase does
not contain a single one. Error Prone's `GuardedBy` check runs at ERROR: give it
`@GuardedBy("theLock")` on a field and every access outside that lock becomes a compile error.

That matters because every detector here is **discovery, not prevention**:

- RacerD infers which locks protect which state and finds races nobody annotated - and it cannot stop
  a fixed race coming back.
- SpotBugs' `AT_*` family finds non-atomic access to shared fields, same limitation.
- The racing-double seam tests only re-prove seams somebody already found by hand.

So the annotation is the only thing that makes a fix permanent. **Write it with the fix**, not as a
follow-up: a locking decision is obvious while you are making it and archaeology a month later. The
check goes from inert to load-bearing one annotation at a time, and no separate project is needed.

If the right fix is `volatile` or removing the sharing outright, there is no lock to name and no
annotation to write. That is fine - the rule is "record the invariant you just established", not
"add an annotation".

## Known shared state, and where its ledger lives

Do not re-derive these; they are measured and recorded.

- **13 RacerD findings** across `state` (7), `metrics` (4) and `internal` (2) -
  `docs/inflight/static-racerd-findings.md`. The CI ceiling is a number in `maven.yml`: fix one and
  **lower `RACERD_MAX_FINDINGS`**, or the lane fails telling you to.
- **The non-volatile offenders** `lastWorkRequestWasFulfilled`, `ConsumerManager.commitRequested`,
  `RetryQueue.closed`, and `AbstractParallelEoSStreamProcessor.lastCommitTime` -
  `docs/refactoring.md`.
- **The torn-read family** - check-then-act and two-read divergence, which are a *different* class
  from unguarded access and are not what `@GuardedBy` addresses.

## `ShardManager.processingShards` is pinned by a test, on purpose

`ShardMapIsNeverReplacedArchTest` forbids assigning that field outside its constructor, and forbids
production callers of its package-private setter. The map is shared unlocked across both threads, so
replacing the *reference* is worse than a torn read of its contents: a reader holding the old
reference accounts work against a map nothing will ever drain. If you need to change that, change the
rule deliberately and say why - do not add an exclusion.
