# CodeQL does not run on a PR based on anything but master

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

CodeQL here is GitHub's **default setup** - configured in repository settings, with no workflow file
under `.github/workflows/` (`ls .github/workflows | grep -i codeql` returns nothing, which is the
whole tell). Default setup analyses the default branch and pull requests **targeting** it. This repo
stacks PRs routinely, so on a stacked PR the analysis simply does not happen.

**The absence is what makes it dangerous, not the gap itself.** A PR whose base is not `master` has
no CodeQL check at all - not a red one, not a skipped one, nothing. On a checks page an absent check
is indistinguishable from one that ran and found nothing, so a reviewer scanning a green list
concludes the code was scanned. This is the shape
[`a-check-that-reports-success-without-having-run.md`](../solutions/workflow-issues/a-check-that-reports-success-without-having-run.md)
describes, applied to a security control.

## How to see it, rather than take this on trust

```bash
# every open PR, its base, and whether CodeQL appears in its checks
for n in $(gh pr list -R astubbs/parallel-consumer --state open --limit 60 \
             --json number --jq '.[].number'); do
  b=$(gh pr view $n -R astubbs/parallel-consumer --json baseRefName --jq .baseRefName)
  q=$(gh pr checks $n -R astubbs/parallel-consumer 2>/dev/null | grep -ci 'CodeQL\|Analyze (')
  printf '#%-5s base=%-38s CodeQL: %s\n' "$n" "$b" "$q"
done
```

Base `master` gets the `CodeQL` roll-up plus its `Analyze (...)` jobs; every other base gets none.

<!-- post-merge: checked-begin -->
**Observed as a controlled change rather than inferred from the docs.** The PR that added
`.github/scripts/*.js` (astubbs/parallel-consumer#409) carried CodeQL while it was based on `master`,
and CodeQL found a real HIGH there - `js/bad-tag-filter` - which is how anyone noticed the mechanism
exists at all. Re-basing that PR onto its parent changed nothing but the base, and its CodeQL checks
vanished. One term, one flip.
<!-- post-merge: checked-end -->

## Why it is not simply "it gets scanned on master anyway"

It does get scanned once the code reaches `master`, and that is the reason this is a `bug` about
signal rather than an emergency about coverage. What is lost is the **pre-merge** property: a finding
that would have blocked a PR instead arrives after the code is already on the default branch. For a
stack, every rung merges upward with no analysis, and the first scan happens after the last one
lands - so the finding names a commit nobody is looking at any more, and blaming it to a PR means
reconstructing which rung introduced it.

## The fix, and why it has not been done

Move from default setup to **advanced setup**: add a CodeQL workflow under `.github/workflows/`
(conventionally named `codeql`, which does not exist here yet - that absence is the current state,
not a broken citation) with `on: pull_request` and no `branches:` filter, so it runs on every PR
whatever its base.

Two reasons it is filed rather than done:

- **It is a repository-settings change as well as a commit.** Default setup must be disabled in
  settings or the two configurations conflict; a workflow file alone does not replace it. That half
  cannot be done from a branch, and it is not reversible by reverting a commit.
- **It changes a security control repo-wide**, so it wants a deliberate decision rather than riding
  along with whatever PR happened to notice. It is also worth deciding at the same time whether the
  advanced workflow keeps default setup's language matrix and query pack, because advanced setup does
  not inherit them - dropping one silently would trade a known gap for an unknown one.

## An outlier this note does not explain

At least one PR based on `master` also showed no CodeQL checks (astubbs/parallel-consumer#408 at the
time of writing), despite touching Java. That does not fit the base-branch explanation and has not
been diagnosed. Re-run the loop above before assuming the rule is exactly "base != master"; there may
be a second condition - a run that never started, a path filter, or an ordering effect - sitting
underneath this one.
