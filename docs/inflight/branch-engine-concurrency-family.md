# The engine-concurrency branch family: what each one was cut to answer

<!-- inflight-type: register -->
<!-- inflight-impact: stranded-work -->

`perf/engine-concurrency` is the base that astubbs#333 (adaptive concurrency) targets, and a dozen
branches feed or fed it. **Their merge status is a command away and is deliberately not recorded
here** - `git merge-base --is-ancestor <branch> origin/perf/engine-concurrency` answers it, and any
copy of that answer is wrong within a day. What no command answers is *what question each branch was
cut to settle*, and that is what was actually being forgotten.

**Read the results column first.** Several of these branches exist only as negative results - a
plausible optimisation measured and refuted - and a refuted hypothesis with no write-up is one the
next session re-runs.

## The thing to check before believing a branch is stranded

A local branch with no remote of its own says **nothing** about whether its commits are pushed.
`perf/bench-arrival-and-key-skew` has no `origin/` counterpart and every one of its commits is
already contained in the pushed base, because it was merged there under a different branch name.
Asking `git branch -a` produced a confident "this work is unpushed and at risk" that was simply
false. Ask `git merge-base --is-ancestor <branch> <pushed-ref>`.

## The measurement branches

| Branch | Cut to answer | What it found |
|---|---|---|
| `perf/bench-arrival-and-key-skew` | What do the engine arms do under a *controlled arrival rate* rather than flat out, and what does key skew cost? | Key ordering costs 0.13% on distinct keys and **3.1x on skewed** ones. The tail experiment ran and what separates the arms is **not** the tail. Carries the arrival-controlled harness the adaptive benchmark needs. |
| `perf/realistic-workload-headlines` | Do the headline figures survive a workload where key ordering actually costs something? | The share-groups 2.5x **does not reproduce**, and the control refutes the obvious explanation. Also caught the vertx arm counting a failed record as completed. |
| `bench/share-groups-arm` | How does PC compare to Kafka share groups, on semantics share groups actually offer? | Share groups looked 2.5x faster and cost the broker 5x for it - then **the 2.5x inverts at 100ms**, and part of the original gap was the vertx arm scoring failed requests. |
| `test/bench-all-engine-arms` | Do Reactor, Mutiny and the proxy engine behave under the same harness? | Arms exist for all three. The Reactor engine **stalls on a publisher that emits nothing**. |
| `perf/direct-pull-measured` | Is the unfinished 2022 direct-pull engine worth having? | Finished it (it never had its blocking wait) and measured: **3x faster at ten workers, 20x slower at five thousand.** |
| `perf/record-residence-time` | What does a caller actually experience, end to end, including queueing and retries? | Added residence-time instrumentation and the load/latency columns - the signal the adaptive design now prefers over the service-time tap, which excludes queue wait. |

## The optimisation branches, and their refutations

These matter most, because each is a plausible idea that was *measured and killed*. The result is the
deliverable; the branch is just where it happened.

| Branch | Hypothesis | Verdict |
|---|---|---|
| `perf/lock-free-mailbox` | The dispatch cost is the worker pool's queue lock | **Retracted.** Tested and it made things **69% worse**. Profiling properly showed the biggest park site is *our mailbox*, not the pool. |
| `perf/lock-free-worker-queue` | Then the lock is the cost, so make the queue lock-free | **Refuted twice.** A lock-free worker queue is **3x SLOWER**, so the lock was not the cost either; and counting the queue does not help - `LinkedTransferQueue` itself is the loss. |
| `perf/split-shard-inflight` | Splitting shard state into selectable and in-flight will speed up dispatch | **10x dispatch, 0% end to end.** The microbenchmark moved an order of magnitude and the thing users experience did not move at all - the sharpest reminder in this family that a component win is not a product win. |
| `perf/resume-shard-scan` | Resuming the unordered dispatch scan beats re-walking the in-flight prefix | Implemented; sits with the rename-correction commits. |

## Why this matters to the adaptive work

Two of these directly shape astubbs#333:

- **The arrival-controlled harness is on the base already**, not waiting to be built. The adaptive
  benchmark needs a merge of `perf/engine-concurrency`, not new work.
- **`perf/split-shard-inflight`'s result is the warning the adaptive benchmark must respect**: a
  controller that moves impressively is not a controller that helps. The claim has to be measured end
  to end at a given arrival rate, never as a component metric.

The adaptive design and its open items are in
[`pr-333-adaptive-concurrency-outstanding.md`](pr-333-adaptive-concurrency-outstanding.md) and
[`core-auto-scaling.md`](core-auto-scaling.md); this file owns only the branch map.

**Delete a row when its branch's knowledge has a durable home** - a `docs/solutions/` write-up or a
committed result file. The refutations above are the rows most at risk: nothing in the code records
that a lock-free worker queue was tried and was three times slower.
