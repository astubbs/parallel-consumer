# The quarantine lane's self-check passes when it found nothing to check

**A gate that certifies more than it measured.** Found 2026-08-11 while sweeping for other instances of the
review-gate defect class.

`bin/quarantine-lane-report.sh:104-132`: the lane-leak self-check enumerates its input with

```
find . -path '*/surefire-reports/*.xml' -o -path '*/failsafe-reports/*.xml'
```

When that returns nothing the loop body never executes, `leak` stays `0`, and line 132 prints:

> Lane-leak self-check passed: every executed testcase matches a registry entry.

So if the Maven step died before producing any reports, the check reports success and the sentence it
prints is stronger than the evidence it gathered. Same shape as the `claude-review` gate that passed while
reviewing nothing: **the verdict is derived from an absence rather than from the work product.**

The near miss is instructive - the same file gets it right elsewhere. `outcome_of` (line 78) distinguishes
a missing report and returns `NOT_RUN`, rendered as "report missing - check the lane job" (lines 153-154).
That path is report-only; the self-check has no equivalent.

**Not urgent:** this job is non-gating, so it cannot produce a false green on merge. It can produce a false
green in a report a human reads, which is why it is worth fixing rather than ignoring.

The transferable test for this class, worth applying to any gate: **name the gate's input, then ask whether
it would still be there had the work never happened.** An artifact the tooling announces itself with fails
that test; an artifact the work produced does not.

Ruled out by the same sweep, with reasons: `check-copyright-headers.sh`, `check-issue-refs.sh`,
`check-action-versions.sh`, `check-shell-sigpipe.sh`, `check-quarantine-registry.sh`,
`check-quarantine-owners.sh`, and both `.github/scripts/*-gate.js`. All derive their answer from the repo
tree or the diff rather than from an artifact emitted by the thing under test.

## Delete when

The self-check distinguishes "no leaks found" from "no reports found", the way `outcome_of` already does.
