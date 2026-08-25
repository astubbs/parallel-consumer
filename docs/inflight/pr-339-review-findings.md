<!-- post-merge: checked -->
# astubbs#339 - the low-disk warner: what the review left open

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->

<!-- post-merge: checked-begin -->
Findings from the simplify-and-review pass over `.claude/hooks/warn-low-disk.sh` that astubbs#339,
the PR that added it, did **not** close. The P0s and P1s that pass found *were* fixed before it
landed; what is below remains. Delete this note when these are resolved.
<!-- post-merge: checked-end -->

## The warner does not reach the agents its own incident describes

The throttle stamp is keyed per-UID, so it is shared across every concurrent session. In the
motivating incident - eleven per-language demo agents taking the host volume from ample to 8.8 GiB
free in about an hour - **only the first agent would see the warning**; the other ten are inside the
ten-minute window somebody else opened. Two reviewers flagged it independently. The fix needs a
decision about keying on `session_id`, which is why it was not made.

Related, smaller: the stamp is written *before* the message is emitted, so a kill between the two
loses that warning for ten minutes.

## `bin/test-check-agent-hooks.sh` failure counts are only meaningful measured serially

<!-- post-merge: checked-begin -->
Measured serially, five runs each: pristine `master` at `da91f3f61` is **154 ok, 0 failures**, and
`da91f3f61` plus astubbs#339 is **189 ok, 0 failures**. An earlier claim that the suite reported 14
failures identical to master's was wrong in both halves - it compared two runs contaminated by
running concurrently with other builds on the same box. **Re-run serially, with nothing else
building, before concluding anything from a failure count here.**
<!-- post-merge: checked-end -->

<!-- post-merge: checked-begin -->
The concurrency failures are real but belong elsewhere. One cause was a shared `TMPDIR`, fixed in
astubbs#339 by giving the push-reminder section a private one; the other is in the merge section and
is owned by astubbs#350. **Those two touch the same lines, so whichever merges second resolves a
textual conflict** - small either way.
<!-- post-merge: checked-end -->

## Never verified against BSD userland

The `stat -f` branch and the `df -Pk` column layout have never run on macOS, which is the platform
the sparse-image path was designed for. Nobody has run them there since; the commands were staged
only in a session scratchpad that no longer exists, so a macOS run starts from scratch. Separately,
bare `mktemp` with no template appears in `bin/test-check-agent-hooks.sh` and fails on BSD `mktemp`;
that is pre-existing and belongs to astubbs#341.

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
