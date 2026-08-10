# Issue and PR reference conventions

How to write a reference to an issue or PR so it names the right repository, links where linking is
possible, and closes what it claims to close. AGENTS.md carries the short rule and the pre-push
check; this is the full convention and the reasoning behind it.

## Why any of this is necessary

The fork's issue numbers sit **entirely inside** upstream's range, so an unqualified `#NN` is a coin
flip. Of the 51 numbers cited across one PR's files, **48 existed in both repos meaning different
things**: `#29` is this fork's rebalance fix and upstream's async-sending request; `#114` is a docs
PR here and a GPG-key issue there.

"fork" is not used as a qualifier either - this repo *is* a fork, so the word names nothing. Nor is
"upstream": that describes a relationship rather than a repository, and it is not stable, since this
fork is itself upstream to anyone who forks it. The gate accepted `upstream #NN` while the tree
still used it; the tree-wide sweep removed the last use and the tolerance went with it, so the form
is now **flagged like any bare number**. It was dropped rather than merely discouraged because a
tolerated form comes back the moment someone copies older text.

## The threshold

Below a threshold, a reference must name its repository; at or above it a bare number is
unambiguous, because only this fork can have one. **`QUALIFY_BELOW` in
[`.github/scripts/issue-ref-gate.js`](../.github/scripts/issue-ref-gate.js) is the source of
truth.** It is expected to move - confluentinc's numbering still creeps - and prose cannot read a
constant, so any number written in a document is a snapshot. If they disagree, the constant is
right and the document is stale: change it there, then sweep the prose.

Upstream numbering is dormant rather than archived, so it still creeps. Measure the headroom rather
than trusting a figure written anywhere:

```bash
gh api 'repos/confluentinc/parallel-consumer/issues?state=all&per_page=1&sort=created&direction=desc' --jq '.[0].number'
```

`state=all` matters, since the highest number is usually a merged PR. `upstream-sweep.sh` warns
when the headroom thins.

## The forms

| Context | Required form |
|---|---|
| Prose, docs and code comments in-repo, below the threshold | `astubbs#NN` or `confluentinc#NN` |
| Link text a human reads | Name the repo in the text too: `[confluentinc issue #12]`, not `[issue #12]` |
| Anything **posted to GitHub** (PR and issue bodies, comments) | Fully qualified `owner/parallel-consumer#NN` |
| Closing keywords | `Fixes astubbs/parallel-consumer#167` |
| Commit and PR titles | Issue at the front: `type(scope) astubbs#NN: subject` (see AGENTS.md, Commits) |

**A hyperlink satisfies the gate, but not the reader - so name the repo anyway in link text.** The
gate can see the target and stops asking; a human reading `issue #12` cannot, and this fork has its
own `#12`. Write `[confluentinc issue #12]`, above all in `README.adoc`, which is the published
artefact and whose audience is *on the fork*. Leave a quoted upstream title intact and append the
number instead (`[Enhanced retry epic confluentinc#65]`) rather than editing the quotation. This is
style, not enforcement - the gate will not flag a bare number beside a URL, which is exactly why it
is written down.

**In anything posted to GitHub, use the fully qualified form.** Upstream prose does not auto-link
there, and a bare `#NN` in a comment silently resolves against whichever repo it is posted in. The
short `astubbs#NN` satisfies the gate but is not cross-reference syntax, so GitHub renders it as
plain text and the body loses the link it would otherwise have had.

**Closing keywords are the exception, and getting this one wrong fails silently.** GitHub honours
only `Fixes #167` or `Fixes astubbs/parallel-consumer#167`. The `owner#NN` short form this
convention otherwise prefers is **not** cross-reference syntax, so `Fixes astubbs#167` renders as
plain text and closes nothing. A bare number is what the convention forbids, so in a PR body write
the fully qualified form - the one spelling that closes the issue, names the repo and auto-links.
The gate reads PR bodies, so the other two spellings now fail it rather than failing silently.

**`Fixes #NNN` only closes on PRs targeting the default branch.** Discovered on astubbs#29, which
targeted `master-confluent`: the keyword was in the body and GitHub ignored it entirely. Check with
`gh pr view N --json closingIssuesReferences` rather than assuming. And never use `Fixes` for a
*partial* fix - see the mirrors for confluentinc#233, confluentinc#326 and confluentinc#857, none of
which their linked PRs actually resolve.

## Choosing the right number

- **Resolve the number in both repos before choosing the prefix** - it very likely exists in each.
  What the gate checks is that a reference *names* a repo, not that it names the right one:
  `astubbs#857` passes and is still wrong. **A wrong reference that resolves is worse than a broken
  one**, because nothing looks amiss. An earlier version of the gate instead asked "does this number
  resolve here?", and so passed `#200` - a real fork issue about ManagedTruth - while the author
  meant confluentinc#200, the shared-nothing architecture.
- **Cite both numbers, fork first**: `(astubbs#119, confluentinc#857)`. The fork number is what
  `Fixes` acts on and what a reader of this repo can open; the upstream number is what four months
  of commits, branch names (`bugs/857-...`) and the upstream threads all use. Dropping either
  breaks a trail.
- **Prefer the fork mirror's number** when one exists (`astubbs#119` mirrors confluentinc#857), and
  put the upstream number in the commit trailers where a tool can read it. See
  [`docs/upstream.md`](upstream.md).

## Fixing references as you go

Not a bulk rewrite - opportunistic, in any file a PR touches anyway:

- every unqualified `#NNN` below the threshold gains its repo - `astubbs#NNN` or
  `confluentinc#NNN` - **hyperlinked** where the format allows (markdown link, javadoc
  `<a href>`, or a raw URL in a `//` comment, which every IDE linkifies)
- resolve the number in **both** repos before choosing the prefix
- add the fork mirror number alongside an upstream one where a mirror exists
- there is no backlog left to work through: the tree-wide sweep qualified every remaining reference
  and converted the last `upstream #NNN` uses, so anything you find now is drift, not leftovers

## The gate, and checking locally

**Check before you push: `bin/check-issue-refs.sh`.** The `PR Checklist` workflow fails a PR whose
*added* lines contain an unqualified `#NN` below the threshold, and finding that out from CI costs a
push cycle for a one-character fix. The script applies the same rule as the gate because it calls
the same `.github/scripts/issue-ref-gate.js` module rather than a second copy, so the rule cannot
drift from CI. It judges the working tree, like `bin/check-copyright-headers.sh`, so uncommitted
edits are caught too. Only lines you *add* are scanned; pre-existing bare refs in a file you touch
are fine.

The check is purely textual - no API calls, so it cannot race issue creation.

The *inputs* differ from CI in two narrow ways. CI reads patches from GitHub's `pulls.listFiles`,
which omits `patch` for a very large diff, and the gate skips a file it cannot see - while the local
script builds its own patch with `git diff` and still checks it. And CI additionally scans the **PR
body**, which does not exist when you run the script. So a green local run promises neither that CI
looked at every file nor that the description passes; a red one is always real.

**The PR body is in scope, and it is the one place the fully qualified form is mandatory.** The body
is the surface people actually read on GitHub, and a bare `#200` renders there as a *working* link
to the wrong issue - the exact failure the gate exists to prevent, on its most visible page. This is
not a second rule: the body is fed to the same `suspectRefs` as a synthetic entry (`prBodyEntry`),
attributed as `<PR body>` in the failure. Fenced code blocks in the body are skipped, because GitHub
does not auto-link inside one either, so a pasted log or a quoted gate failure is not a violation.
Editing the body re-runs the job, so a fix there needs no push.

The files listed in `EXEMPT_PATHS` are exempt, because a bare number legitimately means upstream in
them: `CHANGELOG.adoc`, `upstream-map.yaml`, `upstream-pr-analysis.adoc`, and the gate's own test
fixtures. If a flagged reference really is fork-local, put `issue-refs: N/A - <reason>` on its own
line in the PR body - which skips the body's own references along with everything else.

Logic and tests live in `.github/scripts/issue-ref-gate.js` and `issue-ref-gate.test.js`.
