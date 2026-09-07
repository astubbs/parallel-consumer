# PR: the revoke-path commit drains first, by handing itself to the control thread

<!-- inflight-type: bug -->
<!-- inflight-impact: data-loss -->
<!-- inflight-labels: concurrency -->

Working note for the fix of the defect astubbs/parallel-consumer#436 diagnosed. The knowledge lives in
[`docs/solutions/logic-errors/the-revoke-path-commit-did-not-drain-the-mailbox-2026-09-07.md`](../solutions/logic-errors/the-revoke-path-commit-did-not-drain-the-mailbox-2026-09-07.md);
this note holds only what is still open once the PR is up.

## Open at the PR

- **Collision with astubbs/parallel-consumer#408 on `tryCommitOffsetsOnRevoke`.** Under this fix the
  poll thread never takes the producer transaction lock in transactional mode, so astubbs#408's
  contended-decline branch has no seam in that mode; what astubbs#408 still owns is the size of the bounded
  wait (`commitLockAcquisitionTimeout`, the same five-minute default the inline path had) and its
  `RebalanceEoSDeadlockTest` amendment. Whichever lands second resolves it;
  [`bug-857-transactional-revoke-wait.md`](bug-857-transactional-revoke-wait.md) carries the detail
  from astubbs#408's side.
- **The produce-side fence check is covered at broker level only.** The unit harness drives the raw
  user function and never reaches `ParallelEoSStreamProcessor#acquireProduceLockRefusingRevokedWork`,
  so the refusal path (a worker getting the produce lock after the served commit) has no unit arm;
  `RebalanceEoSDeadlockTest`'s duplicate check is what measures its outcome, 5/5 both ways. A
  unit-level seam would need the wrapper extracted - the `todo` on `processAndProduceResults` already
  asks for that.
- **The eager-processing mode's side effects.** With `allowEagerProcessingDuringTransactionCommit`
  the user function has already run when the fence refuses to produce; that is the at-least-once the
  mode accepts for side effects, and it is now stated in the guard's javadoc rather than only
  implied. Nothing to do unless that mode's contract is revisited.
