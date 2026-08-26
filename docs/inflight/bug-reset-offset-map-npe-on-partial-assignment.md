# resetOffsetMapAndRemoveWork can NPE on a partial-assignment error path

<!-- inflight-type: bug -->
<!-- inflight-impact: crash -->
<!-- inflight-labels: concurrency -->

Surfaced by the torn-read hunt of 2026-08-24 as an out-of-family finding, and split out of
[`bug-torn-read-family.md`](bug-torn-read-family.md) so it is not deleted with that dossier when the
family's work closes.

`resetOffsetMapAndRemoveWork` dereferences state that a partial-assignment error path can leave
absent, throwing `NullPointerException`. The hazard is where it throws from rather than the throw
itself: the rebalance-listener path runs inside `consumer.poll`, so an escape there is the
poller-death shape rather than a contained error - the same consequence class as
astubbs#345's revoke-sweep NPE, by a different route.

Not reproduced. Whoever picks it up should check whether astubbs#345's single-read `getShard(key)`
idiom fix already covers this call path or merely resembles it.
