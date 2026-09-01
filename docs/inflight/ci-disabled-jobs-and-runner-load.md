# Disabled CI jobs, and highcpu runner load

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->
<!-- inflight-state: deferred - after v6, runner capacity rather than release correctness -->


<!-- post-merge: checked-begin -->
- **`Kafka Compat (experimental 4.x)` is disabled** (`if: false` in `maven.yml`) - it cannot compile
  under kafka-clients 4.x until the 0.7.x migration. Re-enable with
  `if: github.event_name == 'pull_request'` when that work starts (see `pr-53-java-baseline-kafka4.md`).
- ~~**The `local` self-hosted PR jobs are disabled**~~ **Resolved 2026-08-06:** `pr-local-fast-feedback.yml`
  was deleted, along with `self-hosted-tests.yml`. Neither had a working runner - `local` had none
  registered at all and `performance` pointed at an offline mac laptop - so both queued until GitHub
  cancelled them. Nothing was lost: the integration and performance suites they ran are required
  checks on every PR and run again on every push to master.
- **The highcpu lane's load is much lower since astubbs#111**, which cut it from six suites per branch to
  two (Performance, Chaos). Both mutation entries moved off-box - one PR-scoped lane now runs on the
  GitHub-hosted gate, the full sweep is dispatch-only - and Unit/Integration were removed as
  duplicates of the hosted gate that were measured as no faster. Jobs had been dying of
  runner-lost-communication (3+ times on astubbs#80 alone) and making chaos timing SLOs noisy; **re-check
  whether that still happens** before spending anything on a shared concurrency group. See
  `ci-mutation-testing.md`.
- **Re-checked 2026-08-17, and it still happens.** Answering the question above with an observation
  rather than leaving it open. In one ~30-minute window the `highcpu` workflow failed on **three
  unrelated branches** - `ci/claude-yml-script-grant`, `fix/concurrent-listener-registration` and
  `docs/inflight-note-currency` - while *succeeding* on two of those same branches minutes either
  side. Failures on unrelated branches with interleaved successes is master-state under rule 2, not
  any PR's doing.
<!-- post-merge: checked-end -->

  The signature is the one this entry already names, not a test failure: in run
  [`32010207847`](https://github.com/astubbs/parallel-consumer/actions/runs/32010207847) the
  `Chaos Pain Suite tests` step logs stop dead at 08:34:16 mid-scenario, the step does not end until
  08:40:40, and it fails with **no `BUILD FAILURE`, no stack trace and no `##[error]`** - the process
  was killed, it did not report anything. A reader grepping that log for a failing test finds
  nothing, which is what makes this class expensive to diagnose twice.

  Context worth keeping with the measurement: several agent sessions were building against the same
  box concurrently. The load driving it is not only CI's.

- **Re-checked 2026-08-25, with a tighter control arm and a second signature.** The 2026-08-17 entry
  argues from unrelated branches minutes apart; this one narrows it to **one branch and a docs-only
  delta**. `Chaos Pain Suite` *passed* on `3c1ff838c`
  ([run 32797902524](https://github.com/astubbs/parallel-consumer/actions/runs/32797902524), 01:32-01:44)
  and *failed* on `8a366ec22`
  ([run 32799585950](https://github.com/astubbs/parallel-consumer/actions/runs/32799585950), 02:02-02:14)
  30 minutes later. The entire diff between those two commits is **one markdown file under
  `docs/inflight/`, +55 lines and no code**, on a branch that touches **zero `.java` files** at all.
  An outcome that flips across a delta which cannot reach the engine is not a regression.

  **The signature differs from the killed-process one above, so grep for both.** This run failed
  loudly, mid-`ChaosRevokeUnderWorkCooperativeIT` (seed `784617418707025255`), with starvation
  symptoms rather than a silent death: records queued over 10s (`ProcessingShard#logSlowWork`),
  repeated `Clean execution pool termination failed - some threads still active despite await and
  interrupt` across a dozen PC instances, and a `RebalanceInProgressException` storm. Those are the
  symptoms of a box that cannot schedule the threads the scenario's timing assumes, and they read
  exactly like a product stall to anyone who has not checked the load first.

  **The mechanism behind both re-checks was addressed later the same day, and then the whole premise
  was removed** - `2ccd3c799` re-keyed the concurrency group off the ref and onto the `box-exclusive`
  matrix key, and `025d0b7ea` then took **everything per-PR off the self-hosted box outright**, after
  measuring that the re-keyed group had a fresh failure of its own (26 of 32 box jobs never ran a
  single step, evicted while pending on a repo-wide queue) and that the box bought only 14% of
  wall-clock over a hosted runner. Nothing triggered by a pull request reaches that host now.

  **So a chaos red on a PR is a new fact, not another instance of this entry - and co-residency is
  no longer even available as the explanation.** The note that owned the confirmation question,
  `ci-chaos-lane-serialised-confirm-no-coresidency.md`, was deleted by `025d0b7ea` as answered; read
  that commit's body for the counts. What stays above is the evidence from before any of it landed,
  which is what makes the before/after comparison possible at all.
  <!-- file-refs: N/A - names the inflight note 025d0b7ea deleted, deliberately, as the record of
       where that question was answered -->

- **A 2026-08-25 red was filed here and did not belong**, which is worth one line because the mistake
  is cheap to repeat: run
  [`32812259117`](https://github.com/astubbs/parallel-consumer/actions/runs/32812259117) looked like
  this signature - logs apparently stopping dead 54 seconds in, no `BUILD FAILURE`, no `Tests run:` -
  and was none of it. The log had been fetched with `gh run view --job --log`, which silently
  returned 990 of ~5000 lines. The artifact showed eight chaos ITs green and
  `ChaosChurnStormIT.churnStormMeetsSlosAndBalancesLedger` red on probe violations; it is recorded as
  the fourteenth sighting in [`bug-857-family.md`](bug-857-family.md).

  **The tell that distinguishes the two is the absence of `Tests run:` - and a truncated log has that
  too.** So the counts that make this entry's signature are only evidence when read from a route that
  cannot truncate.
  [`gh-run-view-log-truncation.md`](../solutions/workflow-issues/gh-run-view-log-truncation.md) owns
  the routes.
