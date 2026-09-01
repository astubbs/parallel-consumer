# Retry economics: blast radius, amplification, and the storm forming

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - interpretation over existing retry and lifecycle state; quarantine action waits on the DLQ decision -->

From the follow-up Codex strategy conversation, weekend of 2026-08-29/30 (breakdown root:
[`core-engine-thesis.md`](core-engine-thesis.md)). Three readings of state the retry machinery
already keeps:

- **Blast-radius quarantine.** `maxRetries=5` treats a record failing quietly and a record pinning
  100,000 units of progress identically. PC knows the difference: same-key records queued behind
  it, whether it holds the commit frontier, how much completed work sits beyond it. Recommend (or
  policy-drive) quarantine/DLQ by the *economic cost of continuing to retry*, not the attempt
  count - **balanced, per the GitHub Codex review, 2026-08-31, against the cost of ABANDONMENT: the record
  blocking 100,000 successors is often the one they all causally depend on, so ranking by blast
  radius alone pushes the most important records toward terminal loss. The policy compares
  retry/recovery cost against abandonment cost, and defaults to escalation (a human decision)
  when the abandonment cost is unknown.** Waits on the DLQ direction (astubbs#149 requirements, astubbs#8 draft); until an action
  exists this ships as the recommendation panel in [`web-control-plane.md`](web-control-plane.md).
- **Retry amplification.** Input 20k records/s, handler invocations 31.4k/s: amplification 1.57x -
  per function, so `customer API 3.71x` stands out against four healthy siblings. Distinguishes
  work amplification from traffic growth, which lag cannot.
- **Storm detection.** The feedback loop (downstream slows -> failures -> retries -> more calls ->
  slower) is visible to the engine while it forms, because it measures both sides. astubbs#333
  already carries the defensive half - the failure-fraction inhibitor that freezes growth when a
  fast-failing downstream masquerades as a fast one; this is the same signal promoted to an
  operator-facing warning before the loop closes.
