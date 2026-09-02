# Transactional battle test - the issue landscape this work sits in

<!-- inflight-type: register -->
<!-- inflight-impact: coordination -->
<!-- post-merge: exempt-file - this note IS astubbs#262's issue map, cited from the PR body. Naming the
     branch and the PR is its subject, not an aside that goes stale; the scope opt-out is the marker for
     that, per bin/check-branch-self-reference.sh. -->

Branch `test/transactional-mode-battle-test`, astubbs#262. This is the map: every reported issue that
touches transactional mode, what this work established about it, and what it did not. Kept because
the issues are scattered across two trackers and a reader arriving at the PR should not have to
rediscover which of them are answered.

Reading order for anyone picking this up: this file, then
`docs/plans/2026-08-07-001-test-transactional-eos-battle-test-plan.md` for the evidence, then
`pr-blockers-and-collisions.md` for the land order.

## The reported hangs, and where each stands

| Issue | State | What this work established |
|---|---|---|
| astubbs#44 / confluentinc#803 | open | **Not tested here.** The only issue upstream ever labelled *verified bug*: a transactional instance times out getting the commit lock when a second instance starts. **Its fix, astubbs#29, has now MERGED** - the revoke path declines the commit lock rather than blocking on it, proved by a deterministic probe across {fix, pre-fix} x {eager, cooperative}, twenty repetitions each. This is the one hang the battle test planned around and deliberately did not reproduce; the chaos scenario that would (Phase B) was deferred behind astubbs#29 precisely so its SLOs would not be calibrated against a deadlocking master, **and that blocker is now discharged** - Phase B is startable. Note the fix closes the cycle only in `PERIODIC_CONSUMER_SYNC`; the transactional mode this suite covers never had the second edge. |
| astubbs#175 / confluentinc#809 | open both sides | **Unclaimed.** No commit on master names astubbs#175 or confluentinc#809. Its stack is inside `close()` -> `doClose` -> `commitOffsetsThatAreReady`, a different entry point from confluentinc#833's steady-state commit, so astubbs#177's fix plausibly covers it but nothing has established that. See `upstream-tell-809-833-the-hang-is-fixed.md`. |
| astubbs#177 / confluentinc#833 | **fixed on master; mirror closed, upstream still open** | Its forensics - ~17 minutes with no commit response produced, processed-records flat - were the broker-poll thread dying, the only producer of commit responses. Fixed 2026-08-19 by four commits titled astubbs#177, chiefly `fe45fddfa`: the poller publishes its own death, so a waiter is released with the poller's exception instead of waiting out a timeout that describes nothing. Not astubbs#100, which removed only two of the routes. The mirror closed 2026-09-01; **confluentinc#833 is still open and still unanswered** - see `upstream-tell-809-833-the-hang-is-fixed.md`. |
| confluentinc#541 | closed | "Transactional PConsumer stuck while rebalancing". Covered by the pre-existing `RebalanceEoSDeadlockTest`, which is also the live probe for the astubbs#44 deadlock - it failed once under a 20-run stress hunt. |
| confluentinc#830 | closed | Retry loop on `InvalidPidMappingException`. **Related to an open hole this work found**: `ParallelEoSStreamProcessor` catches that exception, closes, and does not rethrow, so the batch is marked *succeeded* and its offsets committed for records never produced. See `bug-eos-swallowed-produce-failures.md`. |
| astubbs#189 / confluentinc#887 | open | A failed batch is always retried with the same records together, contradicting the batching docs' "no guarantee they will be retried in the same batch". **Untested.** Distinct from C3, which is about recombination across a *crash and replay* - a different path. Worth its own test. |
| astubbs#241 / confluentinc#144 | open | "ProducerManager should handle different types of transaction failures appropriately". Directly adjacent: this work found two such mishandlings (the throwing produce callback, fixed in astubbs#261; the swallowed `InvalidPidMapping`, recorded). Both astubbs#261 and astubbs#257 have now merged, so "what landed" is master - worth re-reading against it. |
| astubbs#232 / confluentinc#40 | open | Feature request for per-record transactions rather than the periodic batch. Not addressed; recorded because the battle test now documents precisely what the *bulk* model does and does not guarantee, which is the input that request needs. |

## Three hang-shaped failures, told apart

`CONCEPTS.md` warns that a stall, a load-tightness flake and an unforceable trigger all present as the
same expired await. Transactional mode now has three known hang-shaped failures with genuinely
different causes, and confusing them has already cost one investigation:

1. **Commit-lock timeout when a second instance starts** - astubbs#44, unfixed, fix in astubbs#29.
   The control thread and poll thread deadlock on `synchronized(commitCommand)`.
2. **The batching stall** - fixed by astubbs#257, merged. At `batchSize >= 2` every batch failed, and because
   only a *success* marks a partition dirty, **no commit was ever attempted**. The offset simply
   froze. Write-up in
   `docs/solutions/test-issues/transactional-batching-stall-produce-lock-released-per-record-2026-08-08.md`.
3. **The poisoned-transaction wedge** - open, design decision. After a terminal produce failure the
   transaction is correctly abortable, every subsequent send is refused, and the instance stays alive
   but stops progressing. Settled from the code: there is no recovery path short of close. See
   `bug-wedged-after-poisoned-transaction.md`.

The distinguishing evidence is what the logs *do not* contain. In (2) there are no commit-path errors
at all, because commits were never attempted rather than attempted and failing. In (3) every send
fails loudly. In (1) the threads are blocked rather than failing.

## What the PR should carry

astubbs#262's body cites this file. When it lands, the entries above that are still open stay here;
the ones this work answered move to `docs/solutions/`, which is where "already happened" lives.
