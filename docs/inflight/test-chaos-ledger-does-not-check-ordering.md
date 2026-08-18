# The chaos ledger checks loss and duplicates, but never ordering

The correctness ledger in `ChaosScenarioBase` (grep `correctness ledger must balance`) already
verifies two of the three things Parallel Consumer promises under churn:

- **no loss ever** - every produced key is consumed at least once
- **duplicates bounded per disturbance**

Plus `ProgressProbe` for liveness: stalls, rebalance-dwell zombies, per-partition lag stagnation.

**It never checks ordering** - and ordering is the headline guarantee. "Key concurrency without
losing per-key order" is the reason the library exists; the suite that hammers it hardest does not
assert it.

## Why this is small

The recording already exists. The ledger receives `allConsumed`, which is the full consumption
history the check needs. Nothing new has to be captured - the assertion is over data already
collected and already passed to `ProgressProbe.ledger(...)`.

## The part that needs care, and is the whole difficulty

**A naive "offsets per key must increase monotonically" will false-positive.** Under at-least-once
delivery with rebalances, a record legitimately reappears: an assignment moves, uncommitted work is
redelivered to the new owner, and an earlier offset is processed again after a later one. That is the
contract working, not a violation - and the chaos suite exists precisely to cause it.

So the check has to state what ordering actually means here, which is roughly: within one key, on one
instance, within one assignment epoch, offsets are processed in increasing order. Redelivery after a
revoke starts a new window rather than breaking the old one. Getting that boundary wrong produces a
detector that fires on correct behaviour - which is worse than no detector, because the suite's whole
value is that a RED means something.

`PartitionState`'s epoch is the natural boundary marker, and the consumption record would need to
carry the instance and epoch alongside the offset if it does not already.

## Related

- `docs/inflight/next-truth-probes-for-internal-state.md` - the same idea one level down: assert
  against independently computed truth rather than the system's own view
- `docs/testing.md`, "Chaos Pain Suite" - what the suite hunts today
- `docs/data/testing-evidence.yaml`, the `chaos` entry
