# Actor-collection revival: ranked candidate directions

> Extracted from `origin/docs/actor-revival-ideation` @427e1dabc, `docs/inflight/next-actor-revival.md`.

The 2022 micro-actor branch family (manifest entry `sweep-2023-actor-ipc` in
[`upstream-map.yaml`](../../src/docs/development/upstream-map.yaml); editorial owner
`docs/refactoring.md` "Actor / IPC message bus" section) went through a grounded ideation pass on
2026-08-17. The full ranked set - six survivors with verified bases, confidence/complexity, and the
rejection record - is the deliverable:

**[`docs/ideation/2026-08-17-actor-collection-revival-ideation.html`](../ideation/2026-08-17-actor-collection-revival-ideation.html)**
(self-contained HTML; open in a browser)

Survivors, one line each (the doc carries the substance - read it before picking):

1. One-day falsification pipeline - rename-script the 4 framework files, compile standalone, run
   2022 tests + a FILO conformance suite; turns the two standing verdicts ("only meaningful as part
   of confluentinc#200", "far too stale to apply directly") into measurements
2. Un-bundle the family - async-produce (confluentinc#356, manifest `sweep-2023-async-produce`) is
   Group B "throughput-critical", not architecture; split `docs/refactoring.md`'s blanket gate
3. Controlled experiment on the confluentinc#857 commit path - mailbox arm vs astubbs#29's lock
   arm, judged by the recorded chaos replay seeds (`docs/inflight/bug-857-family.md`)
4. Accession & graded register - DONE 2026-08-17 for the manifest half (astubbs#305); per-branch
   readiness grades still open
5. Skeleton-first strangler - land the six thread-ownership interfaces as a pure refactor; the
   mailbox becomes a per-seam swap
6. Concurrency mass budget - ArchUnit ratchet on primitive counts; conversions graded on locks
   removed

Key facts the ideation verified (so nobody re-derives them): the framework proper is 537 lines in
4 files coupled to PC by one 16-line marker interface; the two 2022 actor bases were never unified
but lambda-actor-bus's interface is a strict superset of poller-bus-actor's; the blocking
`futureSend.get` that confluentinc#356 targets is still in master source.

Open decision: which survivor (or sequence) to take first. The doc's top pick is 1 (cheapest,
falsifies both verdicts before any architectural bet); the real fork in the road is 3 vs 5.
