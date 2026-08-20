# Handoff: the open stack as of 2026-08-20 07:40Z

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

Written at the end of a long session. Facts and next actions only.

## Merged tonight

`astubbs#323` -> `astubbs#324` -> `astubbs#325`, in that order, all rebase- or squash-merged.
Master is at the three chaos-instrumentation commits.

## Open

| PR | Branch | State | Next action |
|---|---|---|---|
| astubbs#326 | `compound/naming-and-method` | open, checks running | needs review + human LGTM |
| astubbs#322 | `fix/909-load-reproduction` | current with master, Fable simplify+review done | **two decisions below**, then LGTM |
| astubbs#57 | `fix/859-metrics-leak-plus-cherrypicks` | unblocked now astubbs#325 merged | fold in the pcmetrics fix, below |

Merge order the owner set: 323 -> 324 -> 325 -> 57 -> 322 -> 267 -> 29. First three done, so
**astubbs#57 is next**.

## Decisions waiting on Antony (do not decide these for him)

1. **astubbs#322: `PCModule.shardManager(WorkManager)` silently ignores its argument once memoized.**
   A second `WorkManager` against one module would share the first's `ShardManager`; before this diff
   each built its own. Every construction path is memoized, so unreachable today. Options: an
   `IllegalStateException` guard, a javadoc note, or leave. **Shipped code.**
2. **The quarantine bar has no mechanical check.** "Evidence, not diagnosis" is policy;
   `flaky - failed once` passes the non-blank check. Making the ledger machine-checkable is a design
   call.

## Unfinished work with everything already prepared

**`fix/pcmetrics-unordered-comparand` must fold into astubbs#57.** One commit, rebuilt on master,
`PCMetricsTest` green (2 tests). A cherry-pick onto astubbs#57 conflicts in `PCMetricsTest.java`:
astubbs#57 adds a `SHARDS_MAX_SIZE` assertion (confluentinc#905) inside the block the pcmetrics
rewrite deletes. **The resolution is already worked out** - take the rewrite, and carry astubbs#57's
assertion into the new structure beside `SHARDS_SIZE`, keyed on `quantityP0 - completedP0`. A saved
copy of the resolved file was in the session scratchpad, which does not survive; redo it from that
description, it is a ten-minute job.

**astubbs#322's highest-value follow-up, deliberately not done:** a collision-count assertion in
`RegistrationRaceStaleResidentIT`. It is the only thing that closes the remaining silent-pass hole -
an unknown heal path evicting residents *without* de-saturating the pipeline. Needs its own
broker-verified change.

**The thirteenth confluentinc#857 sighting is recorded** in astubbs#326, seed
`2801529966526445415`, marked unresolved. Nobody has run the uncontended replay that would settle it,
and the green side of that discriminator needs two or three replays.

## Things that will bite a fresh agent

- **`gh` resolves to confluentinc here.** Always pass `-R astubbs/parallel-consumer`.
- **Never work in the main checkout**, and never touch a worktree you do not own - several are live.
- **A clean merge is not evidence.** It silently duplicated a note, appended a stale tag block under a
  corrected one, and reverted a moved block, all in this session. Verify supersets by grepping for a
  marker from each side.
- **`./mvnw ... -pl <module>` needs `-am`**, or `ReactorModuleConvergence` fails because the parent is
  not in the reactor.
- **In a git worktree `.git` is a FILE**, not a directory - root-detection walking up on `isDirectory`
  lands on the main checkout.

## Delete when

astubbs#322, astubbs#326 and astubbs#57 have all merged.
