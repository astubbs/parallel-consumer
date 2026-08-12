# astubbs#263 + astubbs#264 - the inactive-test stack

Two stacked PRs. **#264 is based on `chore/audit-inactive-tests`, not `master`** - merge #263 first,
then #264. `Check PR Dependencies` fails on #264 by design while #263 is open; that is not a defect.

Both branches had `master` merged into them on 2026-08-13 (merge commits `f5e72f41` on #263,
`0e625866` on #264). Both were `DIRTY`/stale before that.

## What each PR is

- **#263 `chore/audit-inactive-tests`** - records only. The audit
  (`docs/test-hardening/inactive-tests-audit-2026-08-08.md`), its plan, and two ledger corrections.
  No test behaviour changes.
- **#264 `test/inactive-test-remediation`** - acts on the audit. The real change is
  `OffsetEncodingTests`: five `OffsetEncoding` values that `assumeWorkingCodec` was branching around
  now assert the degraded contract (*work is repeated, nothing is lost*). Plus four dead tests
  deleted, the last JUnit 4 usage off the test compile path, and a `PartitionStateManager`
  javadoc correction (javadoc only - it claimed to truncate offsets on commit; it does not).

## Merge conflicts already resolved - do not re-litigate

`master` rewrote the `AGENTS.md` documentation-map table and renamed `docs/TODO_INDEX.md` →
`docs/todo-index.md` and `docs/QUARANTINED_TESTS.md` → `docs/quarantined-tests.md`.

- #263 took master's table wholesale and re-inserted one `docs/test-hardening/` row after
  `docs/quarantined-tests.md`.
- #264 inherited that resolution, and its edit to the old uppercase `docs/TODO_INDEX.md` was
  re-sited onto `docs/todo-index.md` (a rename/modify conflict).
- Stale path references inside both branches' own docs were repaired **address-only**, per
  `docs/citations.md`: a dated record's claims are not rewritten, but a reference that no longer
  resolves is.

## Open items

- **U6, U7, U8 are deliberately not started** - re-enabling the two long-dark core tests
  (`ParallelEoSStreamProcessorTest.processInKeyOrder` and its sibling) and writing the one missing
  test, `userSucceedsButProduceToBrokerFails`. All three are blocked on astubbs#260, which rewrites
  both files they touch. The research is done and recorded in
  `docs/plans/2026-08-08-002-test-inactive-test-remediation-plan.md`: both tests fail 100%
  deterministically, the library is correct, and the measured commit sequences are in the plan as
  retarget targets. They need assertion surgery, not investigation.
- **The audit is a dated record and has started to drift.** It records
  `grep -c "^- \[ \]" docs/quarantined-tests.md  # 0`; that returns **3** today. Left as-is
  deliberately - the registry gained entries after 2026-08-08. Anyone re-running its reproduction
  commands should read the date first.
- **Local Java verification after the merge was not completed.** The merge, the four repo gates
  (`todo-index --check`, copyright, docs-data, quarantine registry) and a conflict-marker sweep all
  passed; the core unit suite was **not** re-run locally afterwards. CI on the pushed head covers it.
- **Undecided: the plan documents.** #263 and #264 together add ~2,400 lines of markdown against
  ~150 lines of added Java. Two of those files are plan documents totalling ~1,210 lines
  (`docs/plans/2026-08-08-001-*` and `-002-*`). `AGENTS.md` says a plan goes stale once its work
  lands. Whether they ship with the code or are cut before merge is an open call.
