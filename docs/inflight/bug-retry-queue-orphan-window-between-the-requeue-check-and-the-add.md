# The failure re-queue's live-epoch check is not atomic with its retry-queue add

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->
<!-- inflight-labels: concurrency -->

<!-- post-merge: checked-begin -->
**Pre-existing on master, not introduced by the declining revoke sweep** - recorded here because a
reviewer raised it on astubbs/parallel-consumer#431, which shipped that sweep, and it had no durable
home.
<!-- post-merge: checked-end -->

## The window

`WorkManager.onFailureResult` re-validates `checkIfWorkIsStale(wc)` against the LIVE partition map
immediately before `sm.onFailure(wc)`, precisely because the earlier staleness checkpoint cannot
carry the decision. That re-validation is still a check-then-act: nothing serialises the controller
thread against the broker-poll thread, and its own comment says so ("no epoch check here can ever be
atomic with the actions").

So a rebalance completing between the check and the add leaves a queue entry whose shard entry the
revoke sweep has already taken:

1. controller passes the live-epoch check;
2. poll thread increments the epoch and runs `ShardManager.removeWorkFromShardFor` for that record -
   the queue holds nothing yet, so nothing is found, and the container leaves its shard;
3. controller runs `ShardManager.onFailure` - under PARTITION or UNORDERED ordering the shard object
   survives an empty shard, so `getShard` still answers present - and adds the container to the
   retry queue.

The result is the queue-only orphan `removeWorkFromShardFor`'s javadoc describes: work is handed out
by scanning shards, so a container in no shard is never selected, never completed and never swept,
while `getQueueSizeAndNumberReadyToBeRetried` keeps counting it as parked for retry and that figure
is subtracted from the shard population the broker-poller load gate reads.

## Why the declining sweep is not where it was fixed

<!-- post-merge: checked-begin -->
The same window was open before astubbs/parallel-consumer#431, when the sweep removed from the shard
first and the queue second. That ordering did clean up an add landing *inside* the sweep; the
queue-first ordering does not, so the window is wider by the few instructions between the queue
removal and the shard removal. The dominant window - any add landing after the sweep has finished -
is identical either way, and that PR touched neither `WorkManager.onFailureResult` nor
`ShardManager.onFailure`.
<!-- post-merge: checked-end -->

## What a fix has to answer

- A residency test in `ShardManager.onFailure` (only add when the container is still the resident at
  its offset) narrows it but is another check-then-act, so it does not close it.
<!-- post-merge: checked-begin -->
- Closing it properly means the shard map and the queue moving under one lock, which the poll thread
  may not wait for - the constraint astubbs/parallel-consumer#431 established. That is the design
  question, and it is why this is a note rather than a patch.
<!-- post-merge: checked-end -->
