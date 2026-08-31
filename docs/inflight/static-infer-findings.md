# Infer: it runs every checker, and it finds defects nobody had pointed it at

<!-- inflight-type: register -->
<!-- inflight-labels: concurrency -->
<!-- inflight-impact: ci -->

`bin/infer-test.sh` runs Infer over `parallel-consumer-core`'s main code in CI (`static: racerd` on
`ubuntu-latest`), gating on an identity ratchet. **One register for one lane**: this file owns what
the lane reports and why each finding matters. It was two files briefly - a RacerD register and a
separate list of the findings the widened run added - split by CHECKER, with `THREAD_SAFETY_VIOLATION`
delegated between them, so a reader asking "what does the lane report?" had to reconcile two notes.

**Why it earns its place when four other engines are already here.** Every one of those needs to be
told where to look - Error Prone's `@GuardedBy` only fires where somebody wrote the annotation, and
the racing-double seam tests can only re-prove seams already found by hand. Infer infers from how the
program actually uses its state, so it reports defects nobody named.

**`config/infer-known-findings.txt` is the live set, and this note deliberately does not restate
it.** A count in prose is stale the first time anyone fixes a finding, and this note would then be
asserting open work that is closed - the exact drift the ratchet's identity keying exists to avoid.
Run the lane for the current state; read below for what each group *is*.

## `THREAD_SAFETY_VIOLATION` - RacerD

| Where | What it is |
|---|---|
<!-- post-merge: checked-begin -->
| `PCMetrics.registeredMeters` | **Refinds a known defect** - the plain `ArrayList` mutated off two threads that the Lincheck PoC turned up unprompted, tracked in [`bug-shared-collections-across-the-poll-boundary.md`](bug-shared-collections-across-the-poll-boundary.md) and fixed by astubbs#57. RacerD names every mutating entry point statically. |
| `RetryQueue` | **Ground nothing else covers.** `this.unique` read via `Map.size()`/`isEmpty()` racing with writes - **since fixed**, identities retired from the ratchet. What remains is `RetryQueueIterator.closed` read/write, a separate defect; `docs/refactoring.md` owns the offender list. The Lincheck lane's open-items note ranks `ProcessingShard` and `RetryQueue` as the next thing to model and says they are *not modelled at all*. |
<!-- post-merge: checked-end -->
| `AbstractParallelEoSStreamProcessor.lastCommitTime` | **New. In no ledger.** A plain `Instant`, written in one method and read unsynchronised by `isTimeToCommitNow()`. Checked: no inflight note, no `refactoring.md` entry - the only mention anywhere is a code excerpt in an unrelated solutions write-up. It sits on the commit-timing path, in a repo that tracks commit-timeout flakes. Unfixed, and the one genuinely new thing here; it wants a look on its own account rather than as a lint entry. |

<!-- post-merge: checked-begin -->
**What RacerD does NOT find, and why that is not a failure.** None of the four torn-read races found
by hand appears anywhere in its output - astubbs#345 and astubbs#346 are check-then-act on a map,
astubbs#337 and astubbs#344 are two-read value divergence. RacerD models *unguarded access to shared
state*, a different class, so which of the four are still unfixed does not move what it can see: the
blind spot is the class, not the instances. fb-contrib reaches the first pair; nothing static reaches
the second. The four are named by their fixing PRs, which stay citable after each lands.
<!-- post-merge: checked-end -->

## `NULLPTR_DEREFERENCE` - Pulse, and two root causes rather than a long list

- **`PartitionStateManager.getPartitionState` returns `partitionStates.get(tp)` unguarded** and is
  dereferenced without a check across most of that class, plus `ProcessingShard` and `WorkManager`.
  **Owned by [`core-stale-arrival-guard-needs-a-null-safety-decision.md`](core-stale-arrival-guard-needs-a-null-safety-decision.md)**,
  which carries the open decision - fail open, or fail closed and fix the fixtures - and the evidence
  that it is a policy about the method rather than a question about one call site. Do not answer it
  here.
<!-- post-merge: checked-begin -->
- **`getEpochOfPartition` is documented nullable** - its javadoc says "or null if not yet assigned" -
  and `OffsetMapCodecManager` unboxes the result into `PartitionState`'s primitive `long` parameter
  on the next line. **It now has its own note,
  [`bug-epoch-null-unboxes-on-partition-assignment.md`](bug-epoch-null-unboxes-on-partition-assignment.md),
  because the ratchet stopped watching it**: the identity was retired on astubbs#57 after Infer
  stopped reporting it, and the code it is about was not touched by that PR. Read that note before
  concluding from a green lane that this is fixed. This entry's earlier claim that it needs no policy
  decision was wrong - null means *not yet assigned*, so the caller has to choose what that means.
<!-- post-merge: checked-end -->

Why NullAway is silent on all of them: it reasons from annotations, and `getPartitionState` carries
none, so it is assumed non-null and never questioned. Pulse infers across the program instead. **A
green NullAway lane is not evidence about these.**

## `INTEGER_OVERFLOW_L2` - bufferoverrun

`BackportUtils.readFully`, on length arithmetic. No prior art anywhere in `docs/`. Likely unreachable
with real Kafka payload sizes, but it is unaudited arithmetic on deserialisation input, so it wants a
read rather than a dismissal.

## What `starvation` did NOT find, which is worth keeping

Infer's deadlock detector reports **nothing** here, including on the confluentinc#857 AB-BA path -
this project's flagship open defect. Two readings, not exclusive: it models nested lock acquisition
while 857's cycle runs through a blocking queue and a commit-response wait, so it is not the shape it
looks for; and a detector that has never fired is not yet evidence of anything. It is enabled because
it is free - zero findings means no baseline to negotiate - not because it is proven.

## Toolchain, and the trap that nearly buried this

Infer v1.3.0, published for **linux-x86_64 and osx-arm64 only** - no linux-arm64, which matters for
the polyglot C++ client (see [`static-polyglot-client-analysers.md`](static-polyglot-client-analysers.md)).

**`infer run -- ./mvnw` does not work here, and the reason is not Infer.** Its Maven integration runs
the build under a JDK of its own choosing; this project requires 17. Established with a two-arm
control: the exact command Infer runs, including the profile it injects, succeeds standalone at JDK
17, while Infer's captured Maven output carries JDK 24+ warnings the JDK 17 run does not emit.
Capturing `javac` directly sidesteps the wrapper that picks the JDK, and that is what the script does.

`-proc:full` is load-bearing on that javac line: with `-proc:none` Lombok does not run, the compile
fails, and Infer reports an **empty analysis rather than an error**.

That diagnosis was recorded as "RacerD is blocked" for several hours before the javac route was
tried. It was never blocked; the first workaround was simply not attempted. Worth remembering when
the next tool "cannot run".

## The ratchet, and why it is an identity set

Keyed on bug type plus `Class.method`. **It was a bare count ceiling first, and that was wrong**: an
independent cross-model review pointed out that fixing one race while introducing another leaves the
total unchanged, so the ceiling passes green on a codebase that swapped one defect for a different
one - the same reports-green-while-it-changed class the lane exists to police. Keying on bug type is
also what let the lane widen from `--racerd-only` to every checker with no schema change: new
checkers simply contribute new bug types.

Class and method rather than a line number on purpose: the SpotBugs baseline this branch deleted was
keyed on class plus method plus bug type and was defeated wholesale by a package rename, and a line
number is worse, invalidated by any edit above it. The count column exists because two findings can
share one method.

Four arms verified: unchanged tree passes; a removed identity reports a new defect by name; an extra
identity reports a fixed-but-unratcheted one by name; and a **same-count swap fails**, which the
ceiling passed. A collation bug found while testing those is worth knowing - `comm` compares sorted
streams and Python's tuple order disagrees with the shell's lexical order, so the first version
reported known findings as new. Both sides are now sorted with `LC_ALL=C sort`.

## Not done

- **Core only.** The other reactor modules are not analysed; nobody has measured whether they add
  anything.
- **The self-test covers the preflight guards, not the verdict.** `bin/test-check-infer.sh` has two
  red arms and a green near-miss over the "cannot run" guards, which run before any Maven or Infer
  work. The **ratchet comparison and the exit-status check are not covered**, because they sit after
  a full analysis and the script resolves a real classpath before reaching them. Covering them needs
  the verdict logic extracted into a function a test can call - a refactor rather than a test.
  Recorded rather than left implicit, because a partial self-test that reads as complete is the same
  failure one level up.

  This gate shipped **without** any self-test while its two siblings shipped with one, and it is the
  only one of the three that reached CI red: review found the classpath resolved over the whole
  reactor (so the file left behind was the last module's, measured against the example module's
  dependencies plus a stale core jar) and Infer's exit status captured but never checked. Both fixed.
  The corrected classpath happened to yield the same finding set, which is luck rather than design -
  and the reason the ratchet keys on identities rather than a total.
- **No suppressions and therefore no registry tiering yet**, because a set this small needs none. If
  it grows it acquires the same contract every other engine here has: an entry with a reason and a
  re-enable trigger, a `profile:` marker, and a ranked top-N. See
  [`static-analysis-rule-profiles.md`](static-analysis-rule-profiles.md).
- **`@GuardedBy` is the ratchet these findings want, and it still checks nothing.** Infer is
  *discovery*: it reports defects on unannotated fields and cannot stop a fixed one coming back.
  Error Prone's `GuardedBy` check is on at ERROR and the annotation is now declared, so the
  dependency is no longer the blocker - but the tree still contains no annotation, and on a
  `ReadWriteLock` the annotation cannot express the invariant at all
  ([`static-guardedby-is-inert-on-readwritelock-guarded-state.md`](static-guardedby-is-inert-on-readwritelock-guarded-state.md)).
  So each finding should land its annotation *with* its fix wherever the lock is a plain monitor.
  Policy recorded in `docs/refactoring.md`, because it is a thing to do while fixing rather than a
  thing to do.

## Delete when

Every group above is fixed and retired from the ratchet, or has its own note. `getEpochOfPartition`
should still go first - it is the smallest - but it does need a decision, which its own note carries.

<!-- post-merge: checked-begin -->
**A retirement from the ratchet is not by itself evidence of a fix.** astubbs#57 retired five
identities in one change: four `PCMetrics` `THREAD_SAFETY_VIOLATION`s that its `metersLock` work
genuinely fixed, and one `NULLPTR_DEREFERENCE` that merely stopped being reachable for the analyser.
`bin/infer-test.sh` cannot tell those apart - "no longer fires" is its only signal - so the check
belongs to whoever shrinks the set: read the code the identity names before deleting its line.
<!-- post-merge: checked-end -->
