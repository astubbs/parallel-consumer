# Five agent-hook commit bodies exist only on `backup/pre-split-322`

<!-- inflight-type: task -->
<!-- inflight-impact: stranded-work -->

The **content** of these five commits is on master; their **bodies** are not, and in this repo a
commit body is where the diagnosis, the rejected alternative and the reasoning live - `AGENTS.md`,
"Read the record you inherit", is built on that. They could not be replayed onto master during the
astubbs#322 split (all collided with astubbs#299, `da049f703`), and **the reconciliation commit that
was said to name them does not exist** - it was lost when the stack was rebuilt, so the claim itself
was the last thing pointing at them.

**Read them before changing `.claude/hooks/check-merge-outstanding-work.sh`** - between them they are
the whole design record for that guard, including two decisions that read as oversights from the
code alone:

    92c9a73c2  ci(hooks): refuse a merge while this session still has background work in flight
    b1b7a4734  refactor(hooks): simplify pass, and fix a self-test suite that could not fail
    459710581  fix(hooks): close the merge guard's full-path bypass and make its documented override reachable
    4d0abec47  fix(hooks): drop the merge guard's live-build arm rather than scope it
    aa3a7f267  docs confluentinc#909: repoint citations at the solutions doc the deleted inflight note became

Verified reachable on 2026-08-25: `origin/backup/pre-split-322` resolves and all five SHAs resolve.
**That is the whole risk** - they live on one unprotected branch with no PR, and deleting it destroys
the only copy.

## Delete when

The bodies are preserved somewhere that survives the branch - a `docs/solutions/` write-up, or the
guard's own header carrying the reasoning - or somebody decides the loss is acceptable and says so.
