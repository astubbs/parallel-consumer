# A duplication finding can fail to reach the PR, leaving only a red tick and a job log

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->
<!-- inflight-vetted: 2026-09-07 - re-read against the tree and the action: `report.js` in astubbs/duplicate-code-cross-check still only `console.log`s a rejected review comment with no summary-comment fallback, and `maven.yml` still runs `dups: clones` (pinned v1.1.1) as a step inside `scan: repo` -->

<!-- post-merge: checked-begin -->
`dups: clones` found a real clone on astubbs#267, failed the build for it, and **could not post it**.
The finding existed only in the job log, and nobody opened the job log.
<!-- post-merge: checked-end -->

## What happened

At head `ea377c435` the engines reported, exactly:

```
MutinyProcessor.java  [136:35 - 167:15]  (31 lines, 233 tokens)
ReactorProcessor.java [118:36 - 149:116]
```

That is the whole of the two engines' `onError` - the same method written twice. The job concluded
**failure**, so the gate did its job. Then:

```
Could not annotate .../MutinyProcessor.java:136 - Validation Failed:
  {"resource":"PullRequestReviewComment","code":"custom",
   "field":"pull_request_review_thread.line","message":"could not be resolved"}
```

GitHub refuses a review comment on a line it cannot resolve in the PR's diff, so the annotation was
dropped. No inline thread, and no summary comment either - the last duplication comment on that PR
was **eight days older** than the clone it was supposed to describe.

## Why nobody noticed

A red `dups: clones` (since the `scan: repo` fold, a red step inside that check) with no comment is
indistinguishable, at a glance, from the other red ticks a
long-running PR accumulates. The finding is one line inside a job log nobody has a reason to open,
because the tool's contract is that it comments.

Then it got worse in the way that matters: an unrelated simplify pass extracted part of the
duplicated block, dropping the clone under the reporting threshold. **The gate went green while the
duplication was still there**, in reduced form. Anyone checking CI from that point on - which is what
happened - sees `dups: clones SUCCESS` and correctly concludes nothing is being reported, while
incorrectly concluding nothing is duplicated.

## What to do about it

**Green on `dups: clones` is not evidence that no duplication was introduced.** It means either
nothing crossed the threshold, or something did and could not be posted. Those are not
distinguishable from the check's tick.

- **When `dups: clones` is red and you cannot find a comment, read the job log** - `gh run view -R
  astubbs/parallel-consumer --job <id> --log | grep -E "lines, [0-9]+ tokens"`. The finding is there.
- **The annotation failure is the fixable part.** The action should fall back to a summary comment
  when the inline annotation is rejected, rather than treating a posted-nowhere finding as reported.
  That is a change to `astubbs/duplicate-code-cross-check`, not to this repo.
- **A shrinking clone silently clears the gate.** Percentage- and threshold-based duplication gates
  have no memory, so partially extracting a duplicate looks identical to removing it. Nothing here
  will tell you the difference.

## The same class of failure, a different cause, on the other engine (2026-09-12)

<!-- post-merge: checked-begin -->
`dups: similarity` hit this on the fluent-API PR, astubbs/parallel-consumer#502, and it was **not**
the line-resolution rejection above. That engine posts one whole-PR comment rather than inline annotations, so there is no
line to resolve; GitHub refused the comment with a 422 because the **body exceeded the maximum comment
length**. The report ends with a `Full similarity report` details block listing every pair above
`ignore_below` for every Java file in the tree, so it grows with the corpus - on a tree this size it
is already several times over the limit, and only moves further over. Measure it with the local
recipe in
[`ci-dup-similarity-cannot-accept-known-duplication.md`](ci-dup-similarity-cannot-accept-known-duplication.md).

So that check stayed red for days with a correct finding nobody could read, reaching the same dead
end as astubbs#267 by a different route - which is the argument for fixing the delivery rather than
either cause.
<!-- post-merge: checked-end -->

**The fix is the same shape and just as small.** `run_action.py` already inspects the POST status
code, already prints the rejection, and already writes the whole report to `message.md`; it simply
treats a comment that was refused as reported and carries on. It needs to drop or truncate the
full-report block when the body will not fit - and say in the posted comment that it did - and the
workflow should upload `message.md` as an artifact so the finding survives whatever GitHub does with
the comment. Both are changes to `astubbs/duplicate-code-detection-tool` and `maven.yml`, not to any
PR that trips the check.

## The shape worth remembering

<!-- post-merge: checked-begin -->
The tools were not blind and the thresholds were not too loose - both are the usual suspects and both
are wrong here. The finding was correct, specific, and lost in delivery. On astubbs#267 the
duplication was eventually caught by a human reading the code, three review rounds after the check
first went red for it.
<!-- post-merge: checked-end -->

Related: [`static-archunit-main-code-rules.md`](static-archunit-main-code-rules.md) covers the other half
of the same question - what these mechanical checks structurally cannot police, as opposed to what
they policed correctly and failed to tell anyone.
