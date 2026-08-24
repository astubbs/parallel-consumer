# astubbs/parallel-consumer#334 - state, and what the next session should pick up

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

Written 2026-08-25 at head `3f9191c75`. **Delete this file when astubbs#334 merges**; it exists only
to hand the branch between sessions, and a stale handoff reads as live.

This file carries **only what is not written down elsewhere**: the branch and stack state, what was
actually verified versus assumed, and the open decision. Everything about the work itself lives in
the notes linked below and is deliberately not repeated here.

## Where the knowledge lives - read these before deciding anything

| Note | What it owns |
|---|---|
| [`next-kafka-streams-foreign-wrappers.md`](next-kafka-streams-foreign-wrappers.md) | The PoC's findings and the deferred-capability table. The oldest and broadest of these. |
| [`streams-coupling-dimensions.md`](streams-coupling-dimensions.md) | **The register of what to prove next**, ranked by what would falsify the approach - including dimension 1's result and the inference it overturned. Start here. |
| [`test-cross-binding-streams-conformance.md`](test-cross-binding-streams-conformance.md) | How to test a binding in N languages without writing the suite N times, and the three extractions it is earmarked for. |
| [`bug-streams-queries-share-one-answer-slot.md`](bug-streams-queries-share-one-answer-slot.md) | A live silent bug on the public query API, and **why its fix must come first**. |
| [`perf-streams-crossing-attribution.md`](perf-streams-crossing-attribution.md), [`perf-crossing-fixed-versus-per-byte.md`](perf-crossing-fixed-versus-per-byte.md), [`perf-crossing-is-cpu-and-serialised.md`](perf-crossing-is-cpu-and-serialised.md) | What a crossing costs, measured. |
| [`perf-streams-crossing-optimisation.md`](perf-streams-crossing-optimisation.md) | The optimisation work, **parked on purpose** until the concept is proven. Do not restart it. |
| [`docs/plans/2026-08-22-002-feat-kafka-streams-foreign-wrappers-plan.md`](../plans/2026-08-22-002-feat-kafka-streams-foreign-wrappers-plan.md) | The original plan. |
| [`docs/plans/2026-08-24-002-feat-streams-invocation-bundling-plan.md`](../plans/2026-08-24-002-feat-streams-invocation-bundling-plan.md) | **PARKED, and its central mechanism was falsified** - `StateStore.flush()` runs after commit. The banner says so; read it before reviving anything from it. |

## Branch and stack state - recorded nowhere else, which is why it is here

- Branch `research/kafka-streams-foreign-wrappers`, level with origin, clean tree.
- **PR astubbs#334 does not target `master`.** Its base is `feats/go-vendored-pc`, and its body
  carries `depends on astubbs/parallel-consumer#340`. So `Check PR Dependencies` is red by design
  and stays red until the parents merge - that is the gate working, not a fault.
- **Five commits behind `origin/master`** at time of writing. Both sides are package-renamed (zero
  `io/confluent` paths on either), so the merge precondition in `AGENTS.md` holds and a catch-up
  merge is ordinary.
- The Codex strategy review was **cherry-picked** (`ceccb1cc9`), not merged. Its branch,
  `docs/codex-strategy-conversation`, is 144 commits ahead of master and conflicted in five files;
  the single commit wanted from it was purely additive. Do not merge that branch to get it.

## CI: what is real and what is expected

14 checks are red and **none of them are this branch's new code**. Three groups, and only the first
is work:

1. **Standing debt, pre-existing across at least three pushes.** `PR Checklist` fails on 76
   unresolved citations and `inflight: tags` on 68 untagged notes. The citation failures split
   roughly: ~26 are a **gate bug** - `bin/check-file-refs.sh` does not strip the leading `@` of a
   CLAUDE.md import, so it reports live files as missing - ~50 are plan documents citing files that
   were planned and never built, and ~13 are genuinely stale paths.

   **`quarantine: audit` was NOT in this group, and is now FIXED.** `PCMetricsTest` carried one
   `@Quarantined` annotation and **two** entries in `docs/quarantined-tests.md`. `origin/master` had
   one entry against the same single annotation, so the drift did not exist there - it arrived with
   this stack via `f4a16a625 test(core) astubbs#242: cherry-pick the metricsRegisterBinding
   re-quarantine from astubbs#116` (originally `eb602ae39`), which added a second entry for a test
   that already had one because the branch it came from did not carry the first. The two were
   describing the same failure from two directions - master's entry the diagnosed mechanism, the
   cherry-pick's its sightings - so they are folded into one entry with **nothing dropped**, and
   `bin/check-quarantine-registry.sh` now exits 0.

2. **Expected state.** `claude-review` and `review: human LGTM` are red because nobody has reviewed
   the PR; `Check PR Dependencies` because the stack is unmerged. Never "fix" these by editing gates.
3. **Not individually confirmed.** The remaining reds (`Unit Tests`, `tests`, `Chaos Pain Suite`,
   `Demo`, `clients: rust`, `clients: typescript`, `deps: whole-tree CVE scan`, `dups: similarity`)
   were sampled, not each opened. The samples showed gate failures rather than Java test failures.
   **Treat that as unverified** rather than as a clean bill of health.

## Verified this session, and how

Java streams suite 53 tests; Python 46 plus 1 xfail; ruff clean; copyright, file-ref (as a
citation check on new notes only) and issue-ref gates green locally. The join was proved **end to
end against a real broker**, not only in unit tests - one topology running a map, a join and a
reduce, 200 calls to each of three Python functions.

Two red-proofs were run and both caught exactly one test each: transposing the joiner's arguments,
and dropping the table value-type guard.

**mypy reports four pre-existing `unused-ignore` errors** in `_transport.py` and `_session.py`,
confirmed unchanged with this session's edits stashed. Not caused here, not fixed here.

## The plan, in order

The first two are settled and sequential; the third has an open decision on it.

1. **Correlate `Get`/`Describe`**, copying the `call_id` pattern that `BuilderCall`/`HandleAssigned`
   already use. Closes the silent bug. The `xfail(strict=True)` test turns into an XPASS failure
   when it lands, which is the signal to delete its marker.
2. **Move user functions off the reader thread**, which closes re-entrancy properly. The
   characterisation tests in `test_streams_reentrancy.py` are then **inverted, not deleted**.
   **Order matters**: doing this before (1) makes things worse, turning re-entrant queries from a
   hang into more concurrent callers contending for one answer slot.
3. **Attack the next dimension.** Which one is the open question below.

## Open decision, asked and not yet answered

The register is ordered by "most likely to falsify", but dimensions 2 and 3 were placed by
*urgency* instead - they are cheap, additive, and will almost certainly work, which makes them
release blockers rather than research risks. Proposed re-cut, awaiting the owner:

- Promote **windowing** to rank 2 (composite windowed keys against a flat `DataType`; `fetch(key,
  from, to)` is a range query where `Get` is point-only).
- Demote many-out and record metadata into a clearly separate "release blockers, not research
  risks" section.
- Add **host-supplied Serdes** as a new candidate, which is not on the register and may be the
  strongest remaining falsifier: a foreign deserializer is called outside processing entirely -
  repartition reads, changelog restore, standby replication - so it does not fit the
  "engine asks, host answers, stream thread waits" shape that every proved dimension shares.

Worth weighing against one caveat: dimension 1 was predicted to be fatal and was not, so
predictions about which dimension has teeth have a demonstrated error rate here.

## Machine-local, not in the repo

The re-entrancy experiment's full report and its standalone reproduction live in this session's
scratchpad (`reentrancy-findings.md`, `reentrancy-concurrent-get-bug.py`). **The findings that
matter are already in the notes above and in commit `3f9191c75`'s message** - the scratchpad is
corroboration, not the record, and it will not survive.
