<!-- post-merge: checked -->
# The low-disk warner: what the review left open

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->

<!-- post-merge: checked-begin -->
Findings from the simplify-and-review pass over `.claude/hooks/warn-low-disk.sh` that astubbs#339,
the PR that added it, did **not** close. The P0s and P1s that pass found *were* fixed before it
landed; what is below remains. Delete this note when these are resolved.
<!-- post-merge: checked-end -->

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

<!-- post-merge: checked-begin -->
**Master has since given this one a mechanism, and it is now the only coverage that exists.** The
`shell: macos` job in `.github/workflows/repo-hygiene.yml` runs every `bin/test-*.sh` on
`macos-latest` under BSD userland and a pinned bash 3.2, by glob rather than a list - so
`bin/test-check-agent-hooks.sh` is in it without anyone wiring it up, and with it the warner's real
`stat -f` arm and its `df -Pk` column read. `REAL_UNAME` is the un-injectable reading, so on that
runner those are genuinely the BSD spellings and not the Linux ones wearing a `PC_DISK_UNAME`
costume. What it still does NOT reach is the Docker Desktop sparse-image path: a hosted macOS runner
has no `Docker.raw`, so `file_blocks` is called over a path that does not exist and the
high-water-mark correction stays unexercised. Treat a green macOS lane as evidence for the syntax
and the column layout, not for the sparse-image branch.
<!-- post-merge: checked-end -->

## Queued: a hazard row for heredoc-inside-command-substitution

<!-- post-merge: checked-begin -->
The `shell: macos` lane caught the self-test dying under bash 3.2 - `$(python3 - <<HEREDOC ... )`,
whose body 3.2 reads as shell text without recognising `#` comments, so one apostrophe in a python
comment sent it hunting for a closing quote to the end of the file. Fixed by writing the heredoc to
a file at top level, so the body is never scanned as shell at all.

`bin/check-shell-hazards.sh` is where that class belongs: silent, version-dependent, and invisible
to ShellCheck, which is its stated admission test. It is not added here because three instances
already exist on master - `check-cve-exclusions.sh`, `check-ossindex-audit.sh` and
`check-shell-hazards.sh` itself - so a row would need those three triaged and marked in the same
change, and each is a shared file another branch may be editing. Doing it needs its own PR.
<!-- post-merge: checked-end -->

## Smaller, still open

- `df` on the fast path has no timeout, so a hung NFS mount freezes the session. Portable bounding
  is awkward - `timeout` is GNU coreutils.
- Linux `docker_root` is hardcoded to `/var/lib/docker`, ignoring `daemon.json`'s `data-root`.
- Silencing is only possible through the `PC_DISK_*` variables, whose own comment frames them as
  test-only. There is no supported way for a user to turn the warner down.

## Worth keeping: the self-test was decorative and passed anyway

The injection case originally used a `$(touch ...)` payload containing a space. `read` splits on
whitespace, so it could never have executed - and the case **passed with every guard deleted**. A
test asserting a security property has to be run against the unguarded code before it is believed.
