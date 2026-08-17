# Distributed throttling - ideation done, direction not chosen

The ask is astubbs#228 (mirror of confluentinc#24), with confluentinc#766 as the production
demand evidence. Ideation ran 2026-08-17 and produced seven ranked, code-verified directions:
[`docs/ideation/2026-08-17-distributed-throttling-ideation.html`](../ideation/2026-08-17-distributed-throttling-ideation.html)
(branch `feats/ideate-distributed-throttling` until merged). Read that before restarting - it
holds the bases, the rejection table, and the autopsy of the dead `features/rate-limiting`
branch (`e9f49d321`), which is still missing from `docs/refactoring.md`'s idea bank.

Two decisions gate any build:

- **Enforcement fork**: per-shard gate + `availableAt` deferral (idea 2) vs Little's Law
  in-flight controller (idea 3). Fine-grained-per-key vs reaches-the-poller-for-free; one
  likely subsumes the other.
- **Who owns the number**: configured share (idea 1), downstream-reported, or adaptive AIMD
  (idea 6) - shapes the SPI signature (idea 5) before anything ships.

Idea 7 (decorrelated retry jitter) is independently shippable and needs neither decision.
Next step when picked up: ce-brainstorm the enforcement fork into requirements.
