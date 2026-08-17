# Distributed throttling - ideation done, direction not chosen

The ask is astubbs#228 (mirror of confluentinc#24), with confluentinc#766 as the production
demand evidence - and it converges with astubbs#227 (mirror of confluentinc#21, dynamic
concurrency control), the fork's oldest open self-tuning ambition. Ideation ran 2026-08-17,
extended 2026-08-18; eight ranked, code-verified directions:
[`docs/ideation/2026-08-17-distributed-throttling-ideation.html`](../ideation/2026-08-17-distributed-throttling-ideation.html)
(branch `feats/ideate-distributed-throttling` until merged). Read that before restarting - it
holds the bases, the rejection table, and the prior-art autopsies. Related abandoned branches
(`features/rate-limiting`, `features/dynamic-concurrency-control`, `feature/auto-tuning-pressure`,
plus upstream draft PR confluentinc#22) are now catalogued in `docs/refactoring.md`'s idea bank.

Decisions that gate any build:

- **Standalone throttle vs self-scaling controller** (idea 8): does rate limiting ship as its
  own feature, or as one signal into an adaptive controller that also replaces `maxConcurrency`
  (astubbs#227)? Capacity limits are discoverable; contractual quotas are not - so explicit
  ceilings and adaptive discovery are complements composed by `min()`, which argues for the
  strategy-menu shape (idea 5) either way.
- **Enforcement fork**: per-shard gate + `availableAt` deferral (idea 2) vs Little's Law
  in-flight controller (idea 3). Internal plumbing, not user policy - pick one, don't offer both.
- "Who owns the number" is dissolved by the strategy menu: partition-share, downstream-signal
  (structured rate-limit exception from the user function), and adaptive all ship as
  implementations of one SPI; users pick per deployment.

Idea 7 (decorrelated retry jitter) is independently shippable and needs none of these.
Next step when picked up: ce-brainstorm idea 8's scope boundary (what ships in the controller
vs the standalone strategies) into requirements.
