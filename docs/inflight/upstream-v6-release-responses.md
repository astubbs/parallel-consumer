# The 0.6.0.0 release still owes a response on these threads

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

The pre-release sweep posted every drafted response on 2026-09-17 (the eleven `issue-response-*`
drafts and the astubbs#337 bundle - each fork mirror, its upstream original, the reporters tagged, the
Maven coordinates and the import-rewriting `sed` in every upstream post). What follows is what the
release **also** answers and nobody has told yet. It was found by joining the upstream references in
`CHANGELOG.md`'s `## 0.6.0.0` section against every upstream thread with no comment from the fork
since the tag; reproduce that join rather than trusting this list, and tick a line when the post is
up. Every response is posted only on the owner's explicit instruction, and follows the settled shape:
open with the reporters' mentions (every participant who reported the same thing, not only the
opener), address them by name rather than "you", the release link, the coordinates block on
upstream posts, one line on what shipped, and the fork mirror's number.

**Upstream issues the release fixed outright, still open there, no announcement yet.** Each mirror
is already closed.

- [ ] confluentinc#875 (astubbs#183) - a record polled after a rebalance was dropped if a stale
  container still held its offset.
- [ ] confluentinc#912 (astubbs#122) - the JStream result stream blocks until close instead of ending
  early.
- [ ] confluentinc#906 (astubbs#194) - `parallel-consumer-mutiny` declares its real Java 17 floor.
- [ ] confluentinc#526 (astubbs#159) - `LongPollingMockConsumer` ships in the main artefact.
- [ ] confluentinc#622 (astubbs#167) - the README retry-delay example's multiplier corrected.
- [ ] confluentinc#833 (astubbs#177) - `OffsetCommitBudgetExceededException`, and the timeout names
  its cause.

**Partly addressed or settled by design - answerable with the honest scope, on both sides unless
noted.**

- [ ] confluentinc#803 (astubbs#44) - the revocation's wait on the transaction lock is bounded, not
  yet declined (astubbs#466; fix follows in astubbs#408). The mirror got its what-shipped comment on
  2026-09-17; the upstream thread has not.
- [ ] confluentinc#809 (astubbs#175) - every known cause of the commit-response timeout is fixed and
  the message names which; the reported symptom itself never reproduced. Mirror commented
  2026-09-17; upstream not.
- [ ] confluentinc#777 (astubbs#173) - settled as by-design: a revocation redelivers in-flight work.
  The documentation reply is what remains; [`upstream-173-revocation-duplicate-processing.md`](upstream-173-revocation-duplicate-processing.md)
  holds the grace-period decision.
- [ ] confluentinc#551 (astubbs#164) - batching over-request: the validation half shipped
  (astubbs#496), the `target - modulo` arithmetic half is open as astubbs#311.

**Upstream PRs absorbed by fork work, with no comment saying so.** One paragraph each: the
equivalent shipped, with credit to the contributor.

- [ ] confluentinc#918, confluentinc#919, confluentinc#920 (singhvishalkr) - the log-noise trims and
  the JDK 17 build doc.
- [ ] confluentinc#901 (johnbyrnejb) - the licence check and `.gitignore`.
- [ ] confluentinc#908 (devingryu) - virtual threads; the fork went its own way in astubbs#360 and
  replied on the issue, never on that PR.

**A question only the owner can answer:** on confluentinc#894, the 2026-09-01 follow-up asks whether
single-record interactive replay should live inside Parallel Consumer or in an external recovery
layer. A design position, not a release announcement.

**Not answerable with a fix, so not on this list:** confluentinc#843 (wait-for-info),
confluentinc#887 (a poison record re-forms the identical batch), confluentinc#310 (the dead-letter
queue itself), confluentinc#915 (pending the batch-composition decision), confluentinc#867 (the
Vert.x 5 major). The 2026-09-08 survey of feature requests with nothing behind them is in the
retired `upstream-items-with-no-fix-and-no-response.md`
(`git show e6a429d87:docs/inflight/upstream-items-with-no-fix-and-no-response.md`);
[`upstream-coverage-completeness.md`](upstream-coverage-completeness.md) owns the standing
obligation it was one pass at.

Delete this note when every box is ticked; nothing in it outlives the posts.
