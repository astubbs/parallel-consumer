# Log verbosity: bounded lines for dropped batches and user-function failures (astubbs#169, astubbs#170)

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->
<!-- post-merge: exempt-file - this note IS astubbs#203's note, so it names its own branch and PR throughout; it is deleted when that PR lands, which is what makes the mentions safe rather than stale -->

Branch `fix/log-verbosity-batch`. Fixes astubbs#169 (confluentinc#631) and astubbs#170
(confluentinc#640) - the same complaint in two classes: a log line interpolated a whole record batch,
so it grew with `max.poll.records` and log tooling truncated away the part that identified the event.

## What a future session needs to know, that `gh` and `git log` will not say

- **Written from scratch, not cherry-picked.** Upstream has two unmerged PRs for this
  (confluentinc#918 for `RemovedPartitionState`, confluentinc#919 for the user-function error line).
  They were read but not taken: they predate the internals reshaped on this fork, and the fork wanted
  one shared summariser rather than two ad-hoc format strings. Mapping lives in
  `src/docs/development/upstream-map.yaml` under `upstream-pr-log-noise` - not here.
- **Two new shared pieces - reuse them, do not re-invent.**
  - `bz.stub.parallelconsumer.internal.utils.RecordBatchSummary` (main) renders a batch as a
    *bounded* line: topic-partition, record count, offset range, and at most `MAX_PARTITIONS_LISTED`
    partitions named before the rest are collapsed to a count. Any new log line that wants to
    describe a batch should go through it. Full object dumps belong at `DEBUG`, which is where both
    call sites now put them.
  - `bz.stub.parallelconsumer.internal.utils.LogCapture` (test) attaches an appender to one class's
    logger, raises its level for the duration and restores it. It exists because "this line must stay
    short" is only true until someone interpolates a collection into it again, and only a test that
    reads the emitted line notices.
- **Three inline copies of that appender dance existed before this branch; only one was converted.**
  `AmbientProbeExtensionTest` is moved onto `LogCapture` here.
  `SubmitWorkToPoolShutdownRaceTest` has two more (`grep -n ListAppender` finds them) and is left
  alone deliberately - it is unrelated to astubbs#169/astubbs#170 and converting it would widen a
  small PR. **Follow-up: convert them, and treat `LogCapture` as the only way to do this.**
- **`AbstractParallelEoSStreamProcessor.java` is a contended file**, so the change there is two lines
  (a `.summariseForLog()` call and a `DEBUG` line); the logic lives in `PollContextInternal` and
  `RecordBatchSummary` instead. Which other open PRs edit it is
  `gh pr diff <n> -R astubbs/parallel-consumer --name-only`'s answer, not this file's.
  <!-- post-merge: checked -->
- **The `DEBUG` line renders user data, so it sits INSIDE `logWithoutEscaping`.** The full
  `PollContextInternal` calls `toString()` on user keys and values, which is user code running on the
  failure path - the exact hazard `ThrowableUtils.logWithoutEscaping` exists to contain. Do not lift
  it out of the lambda to "tidy up".
- **The `LogCapture` overlap with astubbs#201 is one-directional, not two copies of a class.**
  This branch adds the only `LogCapture` that exists; `fix/155-load-factor-noise` has *the same logic
  inline and private to* `LoadFactorCeilingReportingTest`, and no class of that name. So there is no
  symmetric "both delete their copy": whichever order they land in, **the one thing to do is convert
  that inline block in `LoadFactorCeilingReportingTest` onto the shared `LogCapture`** - if this
  branch lands first, astubbs#201 does it on rebase; if astubbs#201 lands first, it is a follow-up
  here. Nothing on this branch needs deleting either way, and no duplicate class can reach master.
  Note the duplicate-code gate cannot see any of this: it diffs each PR against master, where neither
  the class nor the inline block exists yet.
- **Anything using `LogCapture` is touching a JVM-shared logger, and the suite can run concurrently**
  (`parallel-consumer-core/pom.xml` passes `junit.jupiter.execution.parallel.mode.default=concurrent`
  to surefire, under `${parallel-tests}` - which the `ci` profile sets false, so the hazard is a
  local-run one). Two distinct hazards, and they need different fixes - do not assume one implies the
  other:
  - *Reading someone else's lines.* Fixed by filtering captured lines on a topic name unique to the
    test. Both capture sites do this (`UserFunctionFailureLoggingTest` on `INPUT_TOPIC`,
    `RemovedPartitionStateTest` on its own randomised topic). It is what lets those tests keep exact
    counts (`hasSize(1)`) instead of relaxing to "at least one".
  - *Flooding everyone else with DEBUG.* Only `@Isolated` fixes this, and only
    `UserFunctionFailureLoggingTest` needs it - it holds the capture open across an `await()`, so the
    window is seconds wide. `RemovedPartitionStateTest`'s window is a single synchronous call.
  Without the `@Isolated`, `closeAfterSingleMessageShouldBeEventBasedFast` and
  `queuedMessagesNotProcessedOrCommittedIfSubmittedDuringShutdown` failed. If those two go
  intermittent again, suspect this first.
