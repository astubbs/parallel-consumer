# RacerD: it runs, and it found thirteen races nobody had pointed it at

<!-- inflight-type: register -->
<!-- inflight-impact: ci -->

`bin/racerd-test.sh` runs RacerD (Meta's Infer) over `parallel-consumer-core`'s main code. Opt-in and
non-gating, following `bin/lincheck-test.sh`'s precedent: the script exists and is runnable, and no
workflow calls it.
<!-- file-refs: N/A - bin/lincheck-test.sh ships with astubbs#347 and is named deliberately as the precedent this lane copies; it is not on this branch yet -->

**Why it earns its place when four other engines are already here.** Every one of those needs to be
told where to look - Error Prone's `@GuardedBy` only fires where somebody wrote the annotation (and
this codebase contains none), and the racing-double seam tests can only re-prove seams already found
by hand. RacerD infers which locks protect which state from how the program actually uses them, so it
reports races nobody named.

## The calibration, stated honestly

**It does not find the four named torn-read races**, and that is not a failure - it is the wrong tool
for them. astubbs#345 and astubbs#346 are check-then-act on a map, and astubbs#337 and astubbs#344 are
two-read value divergence. RacerD models *unguarded access to shared state*, which is a different
class. fb-contrib reaches the first pair; nothing static reaches the second.

What it does find, from 73 source files, is thirteen findings in three groups:

| Where | Count | Status |
|---|--:|---|
| `PCMetrics.registeredMeters` | 4 | **Refinds a known defect** - the plain `ArrayList` mutated off two threads that the Lincheck PoC turned up unprompted, tracked in [`bug-shared-collections-across-the-poll-boundary.md`](bug-shared-collections-across-the-poll-boundary.md) and fixed by astubbs#57. RacerD names all four mutating entry points statically. |
| `RetryQueue` | 7 | **Ground nothing else covers.** `this.unique` read via `Map.size()`/`isEmpty()` racing with writes, and `RetryQueueIterator.closed` read/write. The Lincheck lane's own open-items note ranks `ProcessingShard` and `RetryQueue` as the next thing to model and says they are *not modelled at all*; `RetryQueue.closed` is separately named as a current offender in `docs/refactoring.md`. Independent corroboration plus six findings beyond it. |
| `AbstractParallelEoSStreamProcessor.lastCommitTime` | 2 | **New. Not in any ledger.** A plain `Instant` field, written in one method and read unsynchronised by `isTimeToCommitNow()`. Checked: it appears in no inflight note and no `refactoring.md` entry - the only mention anywhere is a code excerpt inside an unrelated solutions write-up. It sits on the commit-timing path, in a repo that tracks commit-timeout flakes. |

So: **one new defect, two independent corroborations of tracked ones, and seven findings in the class
that was next on somebody's list.** None of them needed a harness, an annotation or a seam name.

## Toolchain, and the trap that nearly buried this

Infer v1.3.0, ~220 MB, published for **linux-x86_64 and osx-arm64 only** - no linux-arm64, which
matters for the polyglot C++ client (see
[`static-polyglot-client-analysers.md`](static-polyglot-client-analysers.md)).

**`infer run -- ./mvnw` does not work here, and the reason is not RacerD.** Infer's Maven integration
runs the build under a JDK of its own choosing; this project requires 17. Established with a two-arm
control: the exact command Infer runs, including the profile it injects, succeeds standalone at JDK
17, while Infer's captured Maven output carries JDK 24+ warnings the JDK 17 run does not emit.
Capturing `javac` directly sidesteps the wrapper that picks the JDK, and that is what the script does.

That diagnosis was recorded as "RacerD is blocked" for several hours before the javac route was tried.
It was never blocked; the first workaround was simply not attempted. Worth remembering when the next
tool "cannot run".

## Not done

- ~~**No CI lane.**~~ **Done - `static: racerd` on `ubuntu-latest`.** An earlier draft of this note
  said the natural home was the self-hosted highcpu runner. That was a preference presented as a
  constraint: `ubuntu-latest` is x86_64, Infer ships a linux-x86_64 binary, and the analysis is
  **11 seconds** over core's 73 files. The only real number is the 953 MB unpacked toolchain, which
  is why the 276 MB tarball is what gets cached and it is unpacked per run - several jobs already
  hold `~/.m2` against a 10 GB repo budget.

  It gates on an **identity set**, `config/racerd-known-findings.txt` - twelve lines, checked in, keyed on
  bug type plus `Class.method`. **It was a bare count ceiling first, and that was wrong**: an
  independent cross-model review pointed out that fixing one race while introducing another leaves
  the total unchanged, so the ceiling passes green on a codebase that swapped one defect for a
  different one. That is the same reports-green-while-it-changed class the lane exists to police.

  Keyed on class and method rather than a line number on purpose: the SpotBugs baseline this branch
  deleted was keyed on class plus method plus bug type and was defeated wholesale by a package
  rename, and a line number is worse, invalidated by any edit above it. The count column exists
  because two findings can share one method.

  Four arms verified: unchanged tree passes; a removed identity reports a new race by name; an extra
  identity reports a fixed-but-unratcheted race by name; and a **same-count swap fails**, which the
  ceiling passed. A collation bug found while testing those arms is worth knowing - `comm` compares
  sorted streams and Python's tuple order disagrees with the shell's lexical order, so the first
  version reported known findings as new. Both sides are now sorted with `LC_ALL=C sort`.
- **Core only.** The other reactor modules are not analysed; nobody has measured whether they add
  anything.

- **The self-test covers the preflight guards, not the verdict.** `bin/test-check-racerd.sh` has two
  red arms and a green near-miss over the "cannot run" guards, which run before any Maven or Infer
  work. The **ceiling arithmetic and the exit-status check are not covered**, because they sit after
  a full analysis and the script resolves a real classpath before reaching them. Covering them needs
  the verdict logic extracted into a function a test can call, which is a refactor rather than a
  test. Recorded rather than left implicit, because a partial self-test that reads as complete is the
  same failure one level up.

  This gate shipped **without** any self-test while its two siblings shipped with one, and it is the
  only one of the three that reached CI red: review found the classpath resolved over the whole
  reactor (so the file left behind was the last module's, and the first reported count of 13 was
  measured against the example module's dependencies plus a stale core jar) and Infer's exit status
  captured but never checked. Both are fixed. The count is still 13 with the corrected classpath,
  which is luck rather than design.
- **No suppressions and therefore no registry tiering yet**, because thirteen findings need no
  tiering. If a CI lane lands and the count grows, it acquires the same contract every other engine
  here has: an entry with a reason and a re-enable trigger, a `profile:` marker, and a ranked
  top-N. See [`static-analysis-rule-profiles.md`](static-analysis-rule-profiles.md).
- **`lastCommitTime` is unfixed** and is the one genuinely new thing here. It wants a look on its own
  account rather than as a lint entry, and until it is fixed it holds two of the thirteen ceiling
  slots.

- **`@GuardedBy` is the ratchet these findings want, and it does not exist yet.** RacerD is
  *discovery*: it infers locks and reports races on unannotated fields. It does not stop a fixed race
  coming back. `@GuardedBy("theLock")` on a field is a locking rule Error Prone's `GuardedBy` check
  enforces at compile time - and that check is on and at ERROR in this build already, examining
  nothing, because the codebase contains no such annotation. So each of these thirteen should land
  its annotation *with* its fix, at which point the invariant is enforced by the compiler rather than
  by whoever remembers it. Recorded as a policy in `docs/refactoring.md` rather than as a task here,
  because it is a thing to do while fixing rather than a thing to do.
