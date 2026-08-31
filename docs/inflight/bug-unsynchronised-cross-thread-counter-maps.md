# The metrics counter maps are plain HashMaps written from more than one thread

<!-- inflight-type: bug -->
<!-- inflight-impact: reliability -->
<!-- inflight-labels: concurrency -->

Surfaced by the torn-read hunt of 2026-08-24 as an out-of-family finding, and split out of
[`bug-torn-read-family.md`](bug-torn-read-family.md) so it is not deleted with that dossier when the
family's work closes.

`slowWorkCounters`, `succeededRecordsCounters` and `failedRecordsCounters` are unsynchronised
`HashMap`s reached from more than one thread. Consumers are metrics-only, so the consequence is
counter drift rather than a data-loss route - but an unsynchronised `HashMap` under concurrent write
can also spin or corrupt its table, which is a liveness risk rather than a reporting one.

Related but distinct from the shared-collection work in
[`bug-shared-collections-across-the-poll-boundary.md`](bug-shared-collections-across-the-poll-boundary.md);
read that first, since a shared fix may cover both.
