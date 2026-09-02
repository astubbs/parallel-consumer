# The rebalance-blocking ArchUnit rule could not see past an interface hop

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->
<!-- inflight-labels: concurrency -->

`ArchitectureTest.rebalanceCallbacksMustNotBlock` walks the call graph from every rebalance callback
looking for blocking calls. Its walk resolved `call.getTarget().resolveMember()`, which yields the
**declared** member - so a call through an interface-typed field landed on the abstract method, which
has no body, and the walk stopped there.

`AbstractParallelEoSStreamProcessor` declares `private final OffsetCommitter committer`. Every
revoke-time commit goes through it. So the rule could not reach `ProducerManager` or
`ConsumerOffsetCommitter` at all - **it was blind to the entire commit implementation, which is the
defect class it was written for.**

<!-- post-merge: checked - names the PR that made the change, which reads the same once merged -->
Widened in astubbs/parallel-consumer#408 to fan out from an interface or abstract declaration to its
PC-owned implementations. The moment it could see, it found three reaches from `onPartitionsRevoked`
that had always been there:

| via | blocking call | why it matters |
|---|---|---|
| `ConsumerManager.retryBackOff(long)` | `Thread.sleep(long)` | a backoff sleep on the poll thread, inside the rebalance callback |
| `ConsumerOffsetCommitter.commitAndWait()` | `BlockingQueue.poll(long, TimeUnit)` | **this is the confluentinc#857 AB-BA edge** - waiting on a queue only the poll thread services, from the poll thread |
| `ProducerManager.lazyMaybeBeginTransaction()` | `synchronized syncBeginTransaction()` | entering a monitor from the callback |

All three are **pre-existing on master** and none is introduced by that PR; they are exempted there
with this file as their owner so the rule can go green while staying honest. Whether each is
*reachable in practice* is exactly what has not been established - a static path is not a live one,
and the `commitAndWait` row in particular may be unreachable because astubbs/parallel-consumer#29's
revoke path declines rather than committing. **That is the work: decide each row, do not assume the
static path is real and do not assume it is not.**

## Two instrument defects fixed alongside, both of which hid things

- **The `synchronized` check could not be exempted at all.** It emitted its event without consulting
  the exemption set, so the only way to make the rule green was to not have the reach - fine in
  principle, useless when the reach is tracked debt.
- **The key was `root => target`, which conflates paths.** Two different routes from one callback to
  the same blocking target shared a key, so exempting one silently exempted the other. Not
  theoretical: astubbs/parallel-consumer#44 deleted `onPartitionsRevoked`'s `Thread.sleep` spin, and
  the callback still reaches `Thread.sleep` via `ConsumerManager.retryBackOff` - a two-part key would
  have re-blinded the rule to the spin that had just been removed, while looking like tracked debt.
  Now keyed `root => via => target`.

## What this says about the rule generally

It was green for weeks while unable to see the thing it names. A gate that cannot be shown to fire on
the defect it exists for proves nothing when it passes - the repo's own rule about negative controls,
applied to a static-analysis rule rather than a test. Worth asking of the other hand-rolled ArchUnit
rules here (`ShardMapIsNeverReplacedArchTest`, `WorkContainerFutureIsWriteOnlyArchTest`,
`TestConventionRules`) whether any of them resolve through an interface and stop.
