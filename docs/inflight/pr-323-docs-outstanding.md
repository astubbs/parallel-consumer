# astubbs#323 - the documentation half: what is still open

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

**Base `master`, independent of astubbs#322** - it was detached so it can merge first under the agreed
order (323, 324, 325, 57, 322, 267, 29). astubbs#324 is stacked on it.

Almost documentation-only, and the exception is worth stating rather than glossing: the diff touches
`ConsumerManager.java`, `ConsumerOffsetCommitter.java` and three workflow files, and **every one of
those hunks is a comment-only citation repoint** - notes renamed from `next-`/`parked-` to area
prefixes, and the references to them updated. No behaviour changes. Delete this note when it merges.

## Closed: the session-index window

This note used to record that the retagged notes landed here while the vocabulary, the tag gate and
the index hook rode astubbs#324 - so between the two merging, master's hook would key on
`inflight-priority`, find nothing, and the "open work" block would go silently empty.

**It is closed.** The tracker's reader now travels with its data: `inject-recorded-knowledge.sh`,
`bin/lib/inflight-tags.sh`, `bin/check-inflight-tags.sh` and its self-test are all in THIS PR,
alongside `docs/inflight/AGENTS.md`. It was not a theoretical risk - `agents: hook self-tests` went
red on this PR for exactly that reason, which is what forced the move.

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
