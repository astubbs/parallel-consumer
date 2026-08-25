# `LOCAL` names a Maven coordinate, and any session on this machine can change what it points at

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

**`bench/run-bisect.sh` resolves `PC_VERSIONS=LOCAL` to `bz.stub.parallelconsumer:*:0.6.0.0-SNAPSHOT`
out of `~/.m2`, which is shared by every worktree and every concurrent session on this machine.**
Whoever ran `mvn install` most recently owns that coordinate, and a sweep already in progress picks up
the change at its next JVM start with nothing anywhere saying so.

**Observed 2026-08-22, twice in one evening.** A sweep measuring a branch's own change had the core jar
replaced mid-cell by another session's build - `cksum` 2784810510 (287,881 bytes) becoming 1082575467
(287,311 bytes). Four rows were taken against code the sweep's author had never seen, and the only
outward symptom was that a column the branch had just added came back blank, which reads exactly like
the entirely normal case of an older release that does not publish that meter.

**A results file gives no way to detect this afterwards.** `pc_version` says `LOCAL`, which was true of
every row and identifies nothing.

## What is in place now

- **`bench/run-bisect.sh` surfaces a `NOTE:` from an arm as a warning**, so "I could not measure
  something you asked for" is said out loud rather than appearing as a dash. That does not prevent the
  swap; it moves the first sign of it from a column nobody rereads to a line in the sweep's own log.
- **A sweep can `cksum` the jar before and after each cell** and void the cell when the two disagree.
  That is what caught the second occurrence. It is a shell loop around the harness, not part of it.

## What would actually fix it

**Record the LOCAL build's identity in the results file** - a `pc_build` column carrying the core jar's
checksum, filled by `prepare()`, empty for a published version. Two rows that disagree are then visibly
two different experiments instead of two repeats, and the check is one `cksum` per resolve.

Better still and more work: **resolve `LOCAL` to a per-sweep coordinate** - install the branch under a
version derived from its commit, so two sessions cannot collide at all. That changes the install step
every bench user already has in their fingers, so it needs deciding rather than doing.

## The rule until then

**Do not run a bench sweep against `LOCAL` while another session may be installing**, and say in any
write-up whether that could be ruled out. `bin/worktree-status.sh` shows the sessions; it does not show
what they are about to build. The reverse also holds and is easy to forget: **installing your own build
silently replaces whatever a running sweep is measuring** - restoring the base build afterwards is
courtesy to the next reader of `~/.m2`, not to the sweep that is running right now.
