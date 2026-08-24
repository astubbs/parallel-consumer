# astubbs#339 - the low-disk warner: what the review left open

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->

Findings from the simplify-and-review pass on this branch that astubbs#339 does **not** close. The
P0s and P1s it found *were* fixed on the branch; what is below remains. Delete this note when these
are resolved, not when the PR merges.

## The warner does not reach the agents its own incident describes

The throttle stamp is keyed per-UID, so it is shared across every concurrent session. In the
motivating incident - eleven per-language demo agents taking the host volume from ample to 8.8 GiB
free in about an hour - **only the first agent would see the warning**; the other ten are inside the
ten-minute window somebody else opened. Two reviewers flagged it independently. The fix needs a
decision about keying on `session_id`, which is why it was not made here.

Related, smaller: the stamp is written *before* the message is emitted, so a kill between the two
loses that warning for ten minutes.

## The PR body's verification claim is false

The body says the suite "reports 14 failures - and those 14 are byte-identical to the set a pristine
`master` worktree reports ... Confirmed by diffing the two failure lists". Measured serially, five
runs each: **pristine `origin/master` at `da91f3f61` is 154 ok, 0 failures**, and this branch is
189 ok, 0 failures. The original confirmation compared two runs contaminated by concurrency. Correct
the body before merge.

The concurrency failures are real but belong elsewhere: a shared-`TMPDIR` cause, fixed here by
giving the push section a private `TMPDIR`, and a merge-section cause owned by astubbs#350. **That
fix touches the same lines astubbs#350 touches and will conflict textually** - a small resolution
either way.

## Never verified against BSD userland

The `stat -f` branch and the `df -Pk` column layout have never run on macOS, which is the platform
the sparse-image path was designed for. Staged commands for a macOS run are in the session
scratchpad. Separately, bare `mktemp` with no template appears in `bin/test-check-agent-hooks.sh`
and fails on BSD `mktemp`; that is pre-existing and belongs to astubbs#341, not here.

## Smaller, still open

- `df` on the fast path has no timeout, so a hung NFS mount freezes the session. Portable bounding
  is awkward - `timeout` is GNU coreutils.
- Linux `docker_root` is hardcoded to `/var/lib/docker`, ignoring `daemon.json`'s `data-root`.
- The critical-band advice tells the agent to look for named volumes and never says what to do on
  finding one.
- Silencing is only possible through the `PC_DISK_*` variables, whose own comment frames them as
  test-only. There is no supported way for a user to turn the warner down.

## Worth keeping: the self-test was decorative and passed anyway

The injection case originally used a `$(touch ...)` payload containing a space. `read` splits on
whitespace, so it could never have executed - and the case **passed with every guard deleted**. A
test asserting a security property has to be run against the unguarded code before it is believed.
