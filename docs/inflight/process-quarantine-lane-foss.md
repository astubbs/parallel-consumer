# Parked: extract the quarantine lane as its own FOSS project

<!-- inflight-type: feature -->
<!-- inflight-state: parked - deferred -->


The `@Quarantined` lane - annotation, enforced registry, owner-claim verification, non-gating CI job,
release blocking, self-tests - is generic; nothing in it is parallel-consumer-specific.

**The differentiator:** the closed loop is enforced *in CI* (the registry cannot drift, the owning PR
must exist and stay open, a merged owner that never re-enabled turns red, releases are blocked) rather
than living in a SaaS dashboard.

**Check for prior art first.** Adjacent, mostly commercial: Trunk.io flaky-test quarantining,
BuildPulse, Datadog Test Optimization, Develocity flaky management, and JUnit Pioneer's
`@DisabledUntil` (date-based, no ownership loop). Would extract as annotation + scripts + a reusable
GitHub Action.
