# confluentinc#809: the sporadic commit timeouts, and the one strand still open

<!-- inflight-type: task -->
<!-- inflight-impact: stranded-work -->


Mirror: [astubbs/parallel-consumer#175](https://github.com/astubbs/parallel-consumer/issues/175).
Upstream: [confluentinc/parallel-consumer#809](https://github.com/confluentinc/parallel-consumer/issues/809).

**Read the upstream thread, not the mirror's summary.** The mirror describes the opening report
(`InternalRuntimeException: Timeout waiting for commit response PT30S`, no diagnosis). The thread's
substance is a *second* reporter, `gtassone`, who debugged it with JDWP, shipped a fix, and had it
merged upstream. That work is already in this tree and the mirror does not say so.

## It is not the same defect as confluentinc#833, though it shares the message

confluentinc#833 / astubbs#177 is "PC runs a while and then **exits**". confluentinc#809 is "PC hits
the same message and then **fails to exit**": `state == RUNNING`, `isClosedOrFailed()` false, both
control threads dead, and a Kubernetes liveness probe with nothing to read. The headline stack is on
the close path (`waitForClose` -> `doClose` -> `commitOffsetsThatAreReady`), not the control loop.
Same symptom string, overlapping triggers, one extra defect that confluentinc#833 never had.

## Where each strand stands at HEAD

| Strand | State |
|---|---|
| Poll thread killed by a rebalance-time commit rejection | Fixed. In `internal/ConsumerOffsetCommitter.java`, `commitDeferringOnRebalance` catches both `RebalanceInProgressException` (astubbs#100) and `CommitFailedException` (astubbs#108) - grep `catch (CommitFailedException e)`, the only one in the tree. `CommitFailedException` is the exception in gtassone's own logs. |
| Poll thread killed some other way, symptom names neither subsystem nor cause | Fixed by astubbs#204. `notifyPollerDied` (declared in the same file, called from `internal/BrokerPollSystem.java`) releases the waiter with the poller's exception at the moment of death. |
| `commitSync` retrying a broker outage forever, stranding the poll thread | Fixed by astubbs#204 (per-call rather than per-attempt budget). This is the closest match to gtassone's trigger: connectivity blips on a large shared cluster, one-second commit interval. |
| Close path aborting before the state transition, leaving an undetectable zombie | Fixed **upstream**, by confluentinc#818, and this tree carries it: in `internal/AbstractParallelEoSStreamProcessor.java`, `doClose` wraps `innerDoClose` and sets the state from a `finally` - grep `this.state = CLOSED`, the only one in the tree, and the comment above the `try` still names the issue. Not fork work, and not astubbs#204's. |
| Poll thread **alive but wedged** - the AB-BA cycle | Still reachable. Owned by astubbs#29 (whose fix had not been observed working when this was written) and `docs/solutions/runtime-errors/revoke-path-commit-deadlock-between-poll-and-control-threads.md`. | <!-- post-merge: checked -->

## What a future session should actually do with this

**gtassone is a better wedge candidate than the one astubbs#204 nominated.** That PR flagged
`dumontxiong` on confluentinc#833 as the possible AB-BA case. gtassone is stronger evidence: he posts
his configuration, and it is `PERIODIC_CONSUMER_SYNC` - the only mode in which the cycle can close -
with 128 partitions, concurrency 64 and a user function running from 100ms to minutes. Anyone picking
up this cycle should read his comments before building a reproducer.

**The mirror's `## Fork status` is stale in three ways**, and it is what a future reader trusts:
it credits astubbs#100 alone and predates astubbs#204; it treats the two 0.5.3.1 changes as upstream
fixes that "did not finish the job" without recording that this tree *contains* both; and it never
mentions the close-path hang, which is the half of confluentinc#809 that is genuinely distinct.

**The resilience question this reopens.** confluentinc#809 is a transient-connectivity report, and
astubbs#204 deliberately made PC give up where it previously hung. With the shipped default
(`offsetCommitTimeout` 10s against the consumer's 60s `default.api.timeout.ms`) exactly one attempt is
reachable, so a blip shorter than the outage now terminates the instance. gtassone said terminating
was acceptable *to him* - the hang was his problem - but that is one user. The decision lives in
`docs/inflight/bug-offset-commit-timeout-does-two-jobs.md` and astubbs#317; this issue is the demand
signal for it, not a separate item.

**No test locks in the close-state guarantee.** Nothing in the suite drives an exception through
`innerDoClose` and asserts the instance still reaches `CLOSED`. It came in as an upstream patch with
the behaviour in a `finally` block, and a later refactor could quietly lose it.
