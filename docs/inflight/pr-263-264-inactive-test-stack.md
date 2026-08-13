# astubbs#263 + astubbs#264 - the inactive-test stack

Two stacked PRs. **astubbs#264 is based on `chore/audit-inactive-tests`, not `master`** - merge
astubbs#263 first, then astubbs#264. `Check PR Dependencies` fails on astubbs#264 by design while
astubbs#263 is open; that is not a defect.

## What each PR is

- **astubbs#263 `chore/audit-inactive-tests`** - records only. The audit
  (`docs/test-hardening/inactive-tests-audit-2026-08-08.md`) and two ledger corrections. No test
  behaviour changes.
- **astubbs#264 `test/inactive-test-remediation`** - acts on the audit. The real change is
  `OffsetEncodingTests`: five `OffsetEncoding` values that `assumeWorkingCodec` was branching around
  now assert the degraded contract (*work is repeated, nothing is lost*). Plus four dead tests
  deleted, the recovered manual procedures (`LoadTest.RECOVERED_VOLUMES`, the
  `TransactionAndCommitModeTest` concurrency ladder, `AmbientProbeExtension`'s environment dump, the
  Vert.x 5xx characterization), the last JUnit 4 usage off the test compile path, and a
  `PartitionStateManager` javadoc correction (javadoc only - it claimed to truncate offsets on
  commit; it does not).

## The package rename is merged into both - do not re-litigate

`master` landed the `io.confluent.*` → `bz.stub.*` rename in astubbs#294. Both branches had already
run the same rename with the same script, so most of the move auto-merged; the sweep
(`grep -rnE 'io[\./]*conflu'`) is clean in every `.java` and `pom.xml`.

The ten conflicts and how they went are in astubbs#264's merge commit message. The three that could
be re-opened by mistake:

- `SampleTestingFailsafePluginInclusionCore`, `JavaEnvTest` and `StringTestUtils` came back as
  rename/delete. `master` only renamed them; this stack **deletes** them (`cadf4c95`). Deletion kept.
- `README.adoc` / `README_TEMPLATE.adoc` took master's wording. The branch still claimed the Java
  packages were unchanged and only the `groupId` moved - the exact claim the rename falsified.
- `bin/rename-packages.sh` took master's copy, which is newer (it adds `--defer-prose`).

Earlier, `master` had also renamed `docs/TODO_INDEX.md` → `docs/todo-index.md` and
`docs/QUARANTINED_TESTS.md` → `docs/quarantined-tests.md`, and rewrote the `AGENTS.md`
documentation-map table; astubbs#263 took master's table and re-inserted one `docs/test-hardening/`
row. Stale path references inside both branches' own docs were repaired **address-only**, per
`docs/citations.md`: a dated record's claims are not rewritten, but a reference that no longer
resolves is.

## Open items

- **Should the new 40,000-message `LoadTest` case auto-run in the required performance check?**
  `bin/performance-test.sh` passes `-Dincluded.groups=performance` and is the "Performance Tests"
  leg of `maven.yml`, a required check on every PR, so `asyncConsumeAndProcessAtVolume` runs there
  automatically at ten times the gating volume. `LoadTest` is a listed 1/20 undiagnosed member of the
  load-tightness flake family at the *gating* volume; the rate at 40,000 has never been measured.
  Raised on astubbs#264 and left open for a human call - the javadoc now states the exposure either
  way.
- **The audit is a dated record and has started to drift.** It records
  `grep -c "^- \[ \]" docs/quarantined-tests.md  # 0`; that returns **3** today. Left as-is
  deliberately - the registry gained entries after 2026-08-08. Anyone re-running its reproduction
  commands should read the date first.

## Settled

- **The last three units are done, here.** astubbs#260 merging removed the only blocker, so the two
  long-dark core tests are re-enabled and the missing one is written, rather than left as follow-on:
  - `offsetsAreNeverCommittedForMessagesStillInFlightLong` - was asserting that *nothing* commits
    while work is in flight. PC does commit the base offset; that is a starting point, not progress.
    Now asserts the frontier, cumulatively: `{3}`, `{3,4}`, `{3,4,6}`.
  - `processInKeyOrder` - was asserting flattened offsets `{0,2,3,6,8}`. Two things were wrong: a
    committed offset is exclusive (where to resume, not the last record done), and partition 1's base
    offset is 4, which the flattened helper reads as progress because it only trims a genesis of 0.
    Now per-partition via `assertCommitLists`, ending `(p0=[3,4], p1=[4,7,9])`.
  - `userSucceedsButProduceToBrokerFails` - new. A produce failure must not commit the offset, and
    the record must be retried.
  All three run across all three commit modes, and each was mutation-checked: breaking the expected
  value reds it. The measurements that were salvaged out of the deleted plan are now *in* these
  tests, with the reasoning in comments, so that salvage file is gone too - an executable assertion
  and a prose copy of the same numbers would drift apart, which is the defect the `todo-index.md`
  count fix in this same PR removes.
- **The plan documents are cut.** All three (`2026-08-08-001`, `2026-08-08-002`,
  `2026-08-12-001`) are deleted: their work landed, and `AGENTS.md` says a plan goes stale once its
  work lands. The one thing that outlived them - the measured commit sequences for the three
  unstarted units - was salvaged to `docs/test-hardening/` first, rather than left to be re-derived.
- **Local verification is done.** Temurin 17, full reactor `test-compile` clean, core unit suite
  328 tests / 0 failures under `-Pci` and 3/3 green thread-parallel.
