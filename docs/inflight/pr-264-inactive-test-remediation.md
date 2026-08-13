# astubbs#264 - inactive-test remediation

Acts on the inactive-test audit (`docs/test-hardening/inactive-tests-audit-2026-08-08.md`), which
landed separately in astubbs#263. That PR is **merged**, so this one is no longer stacked: GitHub
retargeted it onto `master`, and `Check PR Dependencies` passes.

## Open: should the new 40,000-message `LoadTest` case auto-run on every PR?

`asyncConsumeAndProcessAtVolume` is `@Tag("performance")`, and the tag is not opt-in the way it
looks: `bin/performance-test.sh` passes `-Dincluded.groups=performance`, and that script is the
"Performance Tests" leg of `maven.yml`, a required check on every PR. So the case runs automatically
at `HIGH_VOLUME_TOTAL` (40,000) - ten times the gating volume - with no retry.

`LoadTest` is a listed member of the load-tightness flake family at **1/20, undiagnosed**
(`docs/inflight/test-load-tightness-flakes.md`), at the *gating* volume of 4,000. The rate at 40,000
has never been measured. If it is anywhere near 1/20 there, roughly 1 PR in 20 gets a spurious red on
a required check, from a scenario the repo has explicitly not classified.

The options, in preference order:

1. **Measure first** - run the uncontended-broker diagnostic from `AGENTS.md` at 40,000 and record
   the rate beside the 1/20, then decide with evidence. The repo's own rule for this family is
   *classify before raising it*, which is what the class javadoc says about the gating volume.
2. **Default the volume to opt-in** - `asyncConsumeAndProcessAtVolume` defaults to the gating volume,
   so the recovered rungs are reached only with `-Dload.total`. The safe holding position until the
   measurement exists; costs the automatic high-volume coverage.
3. **Leave it** - the performance lane does what it exists for, and a real flake surfaces on a
   required check.

Raised on the PR and left for a human call. The javadoc states the exposure either way, so nothing
is silently misleading while this is undecided.

## Inherit: the audit is a dated record, and has started to drift

`inactive-tests-audit-2026-08-08.md` records `grep -c "^- \[ \]" docs/quarantined-tests.md  # 0`;
that returns **3** today, because the registry gained entries after 2026-08-08. Left as-is
deliberately - a dated record's claims are not rewritten, only references that no longer resolve
(`docs/citations.md`). Anyone re-running its reproduction commands should read the date first.
