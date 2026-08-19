# astubbs#323 - the documentation half: what is still open

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

**Base `master`, independent of astubbs#322** - it was detached so it can merge first under the agreed
order (323, 324, 325, 57, 322, 267, 29). astubbs#324 is stacked on it.

Almost documentation-only, and the exception is worth stating rather than glossing: the diff touches
`ConsumerManager.java`, `ConsumerOffsetCommitter.java` and three workflow files, and **every one of
those hunks is a comment-only citation repoint** - notes renamed from `next-`/`parked-` to area
prefixes, and the references to them updated. No behaviour changes. Delete this note when it merges.

## The one that needs a decision: this PR breaks the session index until astubbs#324 lands

The retagged notes land **here**; the vocabulary, `bin/check-inflight-tags.sh`, the hook rewrite and
`docs/inflight/AGENTS.md` all ride **astubbs#324**. Verified against master:
<!-- file-refs: N/A - the tag machinery named here ships in astubbs#324 - naming what this PR does not carry is the point of the entry -->

- master's `.claude/hooks/inject-recorded-knowledge.sh` keys on **`inflight-priority`** and contains
  **zero** references to `inflight-impact`;
- this PR removes those priority markers.

So in the window between the two merging, **the session-start "open work" block reads tags that no
longer exist and goes silently empty**, and `docs/inflight/AGENTS.md` on master documents a retired
scheme. Silence that reads as "nothing open" is exactly the failure class the index was built to
prevent.

**Two ways out.** Merge astubbs#324 immediately after this one and accept a short window; or move the
four machinery files (`bin/lib/inflight-tags.sh`, `bin/check-inflight-tags.sh`, the hook, and
`docs/inflight/AGENTS.md`) down into this PR so the tags and the thing that reads them land together
- the same call already made for the quarantine rule on astubbs#322. **Undecided.**
<!-- file-refs: N/A - these are the astubbs#324 files this entry proposes moving; they do not exist here yet, which is the decision being recorded -->

## Also open

- **One editorial overstatement, flagged not fixed:** the rule-1 rewrite says the owner-granted
  exception "had become the routine path". The record shows exactly **one** exception. The rule change
  stands on its own; the sentence overstates its evidence.
- **`STRATEGY.md` repeats two concrete incidents that `docs/compound-engineering.md` owns.** Arguably
  a claims document earns its own evidence; left alone deliberately.
- **Unverifiable as written:** the CI failure values quoted in the PCMetrics note (internally
  consistent with the test's constants, not re-derivable), and the third NPE-ing fixture in the
  stale-arrival note, which is never named.

## Already fixed

Three echoes of the abolished quarantine rule 1; the PCMetrics note's claim that the test has "no
commit mode" (the base sets `PERIODIC_CONSUMER_SYNC`); a wrong attribution of
`check-merge-outstanding-work.sh` to astubbs#322 when it ships with astubbs#324; three
`check-file-refs` failures on citations of that not-yet-landed hook; and the retag sweep - 23 notes
re-tagged, 27 filenames stripped of status, citations repointed.
