# Transactional retry can report commit success without committing

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

`ProducerManager#commitOffsets`'s retry branch can set `committed = true` without ever calling
`commitTransaction()`: when a retry finds `isTransactionCompleting()` false, it consults only the
*message* of the previous attempt's saved error ("Invalid transition attempted from state READY to
state COMMITTING_TRANSACTION") and otherwise assumes the transaction completed between the
interrupt and the retry - an assumption nothing verifies. A phantom success reports offsets as
committed that may not be.

Found by the astubbs#317 code review (reliability reviewer, confidence 100) and then validated as
**pre-existing**: the branch is byte-for-byte identical at the fork of the seam work (only
`retryCount` was renamed), so it was inherited, not introduced. It matters more since the seam
landed, because commit success now also releases the seam's pause and resets the exhaustion streak
- `commitOffsetsThatAreReady()`'s success path - so a phantom success un-pauses a failing instance
and wipes the handler's history without a real commit behind it.

The seam's own recovery path shows the shape of a fix: `recoverExhaustedTransactionIfPending`
verifies READY via `ProducerWrapper#isTransactionReady()` instead of trusting an error-message
grep, and re-syncs the wrapper's tracked state (`markTransactionCompletedExternally`). The retry
branch should do the same: confirm the producer's actual state before declaring success, and treat
"not completing, not READY" as a failure to keep retrying rather than a success to assume.

`ProducerManagerCommitBudgetTest` names this branch in a comment and deliberately avoids
exercising it; a fix should give it a direct test.

## Delete this file when

The retry branch verifies producer state before setting `committed = true`, with a test that fails
on the old assumption path.
