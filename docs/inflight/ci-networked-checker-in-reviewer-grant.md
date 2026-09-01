# `bin/check-quarantine-owners.sh` reaches the network inside the reviewer's grant

<!-- inflight-type: bug -->
<!-- inflight-impact: security -->


`bin/AGENTS.md` states the rule for the two granted prefixes: **do not give that prefix to a script
that writes, publishes, deploys, or reaches the network beyond `gh` reads.** One script breaks it.

`bin/check-quarantine-owners.sh` fetches twice - the owning PR's base, and its merge preview - and
reads each through `FETCH_HEAD`:

```
git --git-dir="$scratch_dir/preview" fetch --quiet --depth=1 --no-tags "$ORIGIN_URL" "$1"
```

(That fetch used to target the working clone, which re-shallowed every worktree of it; fixed by
routing it into a throwaway git dir. The *network* reach this note is about is unchanged.)

It carries the `check-` prefix, so `Bash(bin/check-*.sh:*)` grants it to the reviewer in both
`.github/workflows/claude.yml` and `.github/workflows/claude-code-review-dispatch.yml` - a job
holding `CLAUDE_CODE_OAUTH_TOKEN`. Nobody approved it; the name is the grant, which is the mechanism
`bin/AGENTS.md` warns the prefix makes silent.

**It is the only one.** A heredoc-aware sweep of every `bin/check-*.sh` and `bin/test-check-*.sh`
finds no other `git fetch`/`clone`/`ls-remote`/`curl`/`wget` outside a quoted heredoc. In particular
`bin/check-cve-exclusions.sh` is **not** an instance, though a naive `grep curl` says it is: its
`curl` sits inside `cat <<'REPRO'` and is printed as remediation guidance, never run. That false
positive was published in an earlier revision of astubbs#286's description and corrected there.

## Why it is still open

Every fix reaches past the change that found it, and this is pre-existing on `master` rather than
something a reviewer-allowlist PR introduced.

- **Refactor the script to drop the fetch.** Cleanest against the rule, but the fetch is
  load-bearing: it is how the check inspects the owning PR's merge preview to see whether the
  `@Quarantined` annotation is actually removed. Dropping it removes the capability, not just the
  network call.
- **Rename it out of the granted prefix** (`bin/quarantine-owner-audit.sh` or similar). Keeps the
  prefix rule literally true - which is what would let a future `bin/`-wide check enforce it - and
  keeps what the script does. Costs a citation sweep: it is named in `docs/quarantined-tests.md`,
  `AGENTS.md`, workflow YAML and its own self-test.
- **Document it as a second considered exception**, beside `bin/test-check-docs-data.sh`. Least
  work, and it weakens the prefix rule to "read-only, except the ones we listed" - the point at
  which the rule stops being mechanically checkable.

An explicit deny is not available: `--allowedTools` has no deny form, so excluding one script means
abandoning the pattern and enumerating every guard by hand, which
`claude-code-review-dispatch.yml`'s own comments argue against at length.

**Lean: rename.** It is the only option that leaves the rule true rather than qualified.

## Delete when

The script no longer matches `bin/check-*.sh`, or no longer reaches the network, or `bin/AGENTS.md`
records it as a disclosed exception.
