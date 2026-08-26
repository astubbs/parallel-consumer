<!-- post-merge: checked-begin -->
# The jcstress probe module: what is still open

<!-- inflight-type: bug -->
<!-- inflight-labels: concurrency -->
<!-- inflight-impact: misdirection -->

`jcstress-poc/` landed in astubbs#348. A review pass over it raised the findings below and that PR
closed none of them. Its measurements live in
[`docs/plans/2026-08-25-002-test-jcstress-poc-plain-long-visibility.md`](../plans/2026-08-25-002-test-jcstress-poc-plain-long-visibility.md),
a dated record whose claims may not be rewritten to match today's understanding
([`docs/citations.md`](../citations.md) owns that procedure) - so the corrections below have to live
here rather than there. Delete this note when they are resolved.
<!-- post-merge: checked-end -->

## The headline safe-direction claim is contradicted by the module's own probe

The plan doc says every anomaly found is **safe** (replay, never skip). But
`BootstrapResetTripleWriteProbes`' `100, 101` outcome describes committing 101, "re-asserting a
pre-reset offset and cancelling the mandated replay". That is a **skip**, not a replay, and it
reproduced on x86-64 as well as arm64.

This feeds the "priority after the release" argument directly, so it is not a wording nit. Suggested
scoping: every anomaly *on the commit path* is safe-direction; the bootstrap-reset `100, 101` outcome
is skip-direction and is contained only by the dirty gate.

## An omitted happens-before edge narrows the finding

The poll thread only reaches `getCommitDataIfDirty` after `maybeDoCommit` polls a `CommitRequest`
off `commitRequestQueue`, a `ConcurrentLinkedQueue` - so add-before-poll **is** an edge, and
everything completed before the commit request is already published. The exposed pair is an
`onSuccess` interleaving a running collection: **one commit cycle wide, not unbounded**. Measured
rates survive as per-raced-pair, but they are not a production incidence.

Found independently by two reviewers, and pointedly: this is the same incidental-fencing class the
faithful arm exists to catch. It found the smaller instance and structurally cannot see the larger.

## The burnt-commit-cycle harm needs two stale reads, not one

`setClean()` clears `dirty` only `if (!stateChangedSinceCommitStart)`. No arm reports that field, so
the second condition is argued rather than measured - the harm is plausible, its rate is not the
measured one.

## Consequences of the deliberate unwiring that the containment claim misses

The module has no `<parent>` and is absent from the root `<modules>`, which is correct and verified.
Three second-order effects follow anyway:

- `jcstress-core` sits outside **both** dependency lanes - dependabot follows `directory: "/"` via
  `<modules>`, and the OSS-index lane runs from the reactor root. That pin will never move and no
  CVE scan will ever see it.
- The workflows' `hashFiles('**/pom.xml')` cache key includes this pom, busting the Maven cache once
  per edit.
<!-- post-merge: checked-begin -->
- **The exclusion costs static analysis too, and this is now the module's own distinction.** It was
  written here while astubbs#356 was still open; that PR has since landed, so the future tense is
  spent. SpotBugs no longer runs `-pl parallel-consumer-core -am` - it covers the whole reactor with
  `includeTests` - and a module reached through neither `<parent>` nor the root `<modules>` is
  outside that sweep, which leaves the probe classes **the only Java in the tree nothing analyses**.
  They are concurrency code. The containment argument still wins and nothing here is a request to
  change it; it is recorded so a later reader sees the cost was priced rather than overlooked, and
  it is the same shape as the dependency-lane bullet above. If the probes ever want analysis, the
  cheapest route is a separate SpotBugs invocation scoped at this module rather than pulling it into
  the reactor, since joining the reactor is what the containment exists to prevent.
<!-- post-merge: checked-end -->

## Nothing enforces reading the positive control

jcstress grades a zero-observation `ACCEPTABLE_INTERESTING` as PASSED, so a run whose actors never
raced prints `[OK]` everywhere. The warning is durable - `jcstress-poc/pom.xml` carries it as `READ
THE CALIBRATION BEFORE BELIEVING ANY ZERO` - but it is prose, and no check makes a vacuous run
distinguishable from a clean one.

<!-- post-merge: checked-begin -->
**The mechanical closure is to assert a count, not to instruct a reader**: have the run fail unless
the calibration arm's observation count is non-zero, which is the same standing instruction
astubbs#356 arrived at from three unrelated instances of the class (an exclude filter matching
nothing, a compiler flag in a profile CI never activates, a mutation control whose arms both failed
for an unrelated reason). Until that exists, this is prose asking for discipline.
<!-- post-merge: checked-end -->

## Nothing detects correspondence drift, or even compiles the module

No probe imports a `bz.stub.parallelconsumer` class (the module's only dependency is
`jcstress-core`), so every probe is a hand-copied replica bound to the real code by nothing but a
human having copied it. If `PartitionState`'s write order changes, nothing goes red.

**This is the prerequisite for growing the module, not a nicety.** The growth model is one
hand-written probe per suspected field pair, which does not scale and rots silently; adding probes
before the check exists multiplies the exposure. A tree-wide check in the style of
`bin/check-file-refs.sh` could assert the modelled fields are still non-volatile and each quoted
snippet still greps in `PartitionState.java`, costing the main build nothing. Named unprobed
candidates: the sibling fields `docs/refactoring.md` records under
`AT_STALE_THREAD_WRITE_OF_PRIMITIVE` with the same "volatile for the flags" fix. **That entry owns
the list** - read it there rather than from a copy, which is how a field fixed in one place goes on
being listed as plain in another.

## Smaller, still open

- **The near-duplicate arms must never be deduplicated - they are the finding.** The reduced and
  faithful arms differ only in their surrounding accesses, and the ~130x suppression between them is
  the measurement; the volatile control arms are near-copies of the arms they control. `dups:
  similarity` reports them as its highest new pairs (topping out at 59.7%, under the 80%
  `fail_above`) and both duplication caps still hold, so nothing needs doing today. What is open is
  that the tool has no allowlist - `ci-dup-similarity-cannot-accept-known-duplication.md` owns that
  hazard - so a future probe class landing above 80% has no way to be accepted, and the pressure will
  be to collapse the arms.
- "The only FORBIDDEN outcomes live in the volatile control arms" is imprecise:
  `CalibrationProbes`' word-tearing probe also carries a FORBIDDEN catch-all and is not a volatile
  arm. The no-CI-lane conclusion still holds.
- **No durable pointer to the module exists** - its only reference is a dated plan doc, which
  `AGENTS.md` says is explicitly not for durable reference.
- **Prior art the plan doc does not cite**: `docs/refactoring.md` already documents this defect class
  on the three sibling fields named above with the same "volatile for the flags" fix, and
  `core-control-thread-contract-debts.md` cautions that fixing piecemeal may conflict with the
  shared-nothing rework.
- **Priority tension**: the plan defers the fix past the release while astubbs#349 implements it
  against an unreleased snapshot. Nothing reconciles the two - Antony's call.
