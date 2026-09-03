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

**The one way it silently checks nothing: a `ReadWriteLock`.** `@GuardedBy` holds a single lock
expression, and naming a `ReentrantReadWriteLock` *field* is accepted and then **not analysed** -
`@GuardedBy("lock")` on `RetryQueue`'s state compiles clean while `size()` reads the map under no
lock at all. Naming `lock.readLock()` does fire, but then every legitimate write-lock-held access
becomes an error too (`instead found: 'this.lock.writeLock()'`), so neither spelling is correct.
**Do not annotate ReadWriteLock-guarded state** - that is `RetryQueue` and `ProducerManager` here.
Plain monitors and `Lock` fields are checked exactly as advertised. Measured both ways, with the
per-site verdicts:
`docs/inflight/static-guardedby-is-inert-on-readwritelock-guarded-state.md`.

If the right fix is `volatile` or removing the sharing outright, there is no lock to name and no
annotation to write. That is fine - the rule is "record the invariant you just established", not
"add an annotation".

## Declare thread confinement with `@ThreadConfined`, and assert it at the entry point

`@GuardedBy` names a lock, so it cannot say "only one thread ever runs this" - and state that is
confined rather than guarded holds no lock to name. Say it with Infer's `@ThreadConfined`, on a
method, a field or a type, which RacerD reads: the accesses inside are taken as confined to that
thread, so they are not reported as racing with each other, while an unannotated access to the same
state elsewhere still is.

**It is a declaration the analyser consumes and never checks, so pair it with a runtime assertion.**
An annotation nobody enforces is a comment that silences a detector, which is worse than no
annotation at all - RacerD believes it and stops looking. `RetryQueue.RetryQueueIterator` is the
pattern: `@ThreadConfined(ThreadConfined.ANY)` on the type, `assertOnOwningThread` at the top of
every entry point, and `RetryQueueIteratorConfinementTest` failing when the two disagree.
`ThreadConfinedConsumer` is the older, hand-rolled version of the same idea for the poll thread.

**`ThreadConfined.ANY` says "one thread, whichever one got here first"**, which is the honest value
when the confining thread varies by caller - an object handed out per call, like that iterator. Name
a thread instead only when the code really does pin one, and then the assertion has something
specific to compare against.

**Check the premise before you write it.** The declaration is only as good as the claim that the
state is confined, and that claim is easy to get wrong from reading one method:
`AbstractParallelEoSStreamProcessor.lastCommitTime` was described as control-thread-confined by
every record that mentioned it, and is written by `tryCommitOffsetsOnRevoke()` on the poll thread.
Grep every writer and every reader of the field, not the two the surrounding code shows you. Where
it turns out not to be confined, `volatile` is usually the answer, and a modifier tripwire is what
keeps it - see "Known shared state" below.

## Record a CLEARED suspicion in the javadoc, not only in the commit

When you investigate a concurrency path and conclude it is safe, **write that conclusion where the
next reader forms the same suspicion** - on the method or field, not only in the commit message or a
`docs/inflight/` note. A dismissal nobody can find is re-derived, and re-deriving it costs the same
as the first time.

The chaos scenarios already do this under a `Calibration status` heading, which is why
`AGENTS.md`'s prior-art checks name a test's own javadoc as a source the six commands cannot reach.
Main code needs it more, because a suspicion here is formed by whoever is reading the code, not by
whoever chose to run a test.

An entry is three things, and the third is what makes it durable:

- **What was suspected**, in the form the next reader will suspect it.
- **The discriminator that cleared it** - the specific fact that decides it, not "looks fine". For a
  lock-ordering suspicion that is what a holder DOES while holding, not that it holds.
- **What would reopen it**, and whether any gate would catch that happening. Say so plainly when
  nothing would: `clearCommitCommand()` carries the worked example, where the ArchUnit rule is green
  whether the invariant holds or not because it cannot see `synchronized` blocks.

**Date it.** A cleared suspicion is a statement about the code as it was, and the reader needs to
know whether it predates the change they are looking at.

## Known shared state, and where its ledger lives

Do not re-derive these; they are measured and recorded.

- **RacerD findings** across `state`, `metrics` and `internal` -
  `docs/inflight/static-infer-findings.md`. The ratchet is an **identity set, not a count** -
  `config/infer-known-findings.txt`, keyed on bug type plus `Class.method`. Fix one and **delete its
  line there**, or the lane fails telling you to. (There is no `RACERD_MAX_FINDINGS`; the bare count
  ceiling was replaced precisely because fixing one race and introducing another left it unchanged.)
- **The non-volatile offenders**, now just `ConsumerManager.commitRequested` -
  `docs/refactoring.md`. `AbstractParallelEoSStreamProcessor.lastWorkRequestWasFulfilled` was fixed
  by astubbs#201 and `lastCommitTime` the same way, both `volatile`, both with a modifier tripwire
  (`PartitionStateDirtyFlagFenceTest` is the pattern) because nothing else goes red when a modifier
  is dropped. **`RetryQueue.closed` came off the list a different way**: the iterator that
  owns it holds a read lock only its opener can release, so it was already confined and the answer
  was to declare and assert that (`@ThreadConfined(ANY)` plus `assertOnOwningThread`), not to make
  it `volatile`. SpotBugs cannot read the declaration and still reports it - one of the two places
  a detector here is now wrong on purpose.
- **The torn-read family** - check-then-act and two-read divergence, which are a *different* class
  from unguarded access and are not what `@GuardedBy` addresses.

## `ShardManager.processingShards` is pinned by a test, on purpose

`ShardMapIsNeverReplacedArchTest` forbids assigning that field outside its constructor, and forbids
production callers of its package-private setter. The map is shared unlocked across both threads, so
replacing the *reference* is worse than a torn read of its contents: a reader holding the old
reference accounts work against a map nothing will ever drain. If you need to change that, change the
rule deliberately and say why - do not add an exclusion.
