# astubbs/parallel-consumer#334 - state, and what the next session should pick up

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

Written 2026-08-25, covering the branch up to and including its own commit. The last piece of work
it describes is moving registered functions off the reader thread; anything in `git log` after that
arrived later than this file and is not accounted for here. **Delete this file when astubbs#334 merges** - it
exists only to hand the branch between sessions, and a stale handoff reads as live.

This file carries **only what is not written down elsewhere**: the branch and stack state, what was
actually verified versus assumed, and the open decision. Everything about the work itself lives in
the notes linked below and is deliberately not repeated here.

## Where the knowledge lives - read these before deciding anything

| Note | What it owns |
|---|---|
| [`next-kafka-streams-foreign-wrappers.md`](next-kafka-streams-foreign-wrappers.md) | The PoC's findings and the deferred-capability table. The oldest and broadest of these. |
| [`streams-coupling-dimensions.md`](streams-coupling-dimensions.md) | **The register of what to prove next**, ranked by what would falsify the approach - including dimension 1's result and the inference it overturned. Start here. |
| [`test-cross-binding-streams-conformance.md`](test-cross-binding-streams-conformance.md) | How to test a binding in N languages without writing the suite N times, and the three extractions it is earmarked for. |
| [`docs/solutions/architecture-patterns/one-answer-slot-for-many-callers-is-a-correlation-bug.md`](../solutions/architecture-patterns/one-answer-slot-for-many-callers-is-a-correlation-bug.md) | **Fixed, so it moved out of `inflight/`.** The silent mis-delivery on the query API, the ordering trap in fixing it, and the two test markers used for a defect versus a limitation. |
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

Java streams suite **73 tests**; Python **51**; ruff clean; copyright and issue-ref gates green
locally. (`file-refs` fails on the standing citation debt below, not on this work.) The join was proved **end to
end against a real broker**, not only in unit tests - one topology running a map, a join and a
reduce, 200 calls to each of three Python functions.

Two red-proofs were run and both caught exactly one test each: transposing the joiner's arguments,
and dropping the table value-type guard.

**mypy reports four pre-existing `unused-ignore` errors** in `_transport.py` and `_session.py`,
confirmed unchanged with this session's edits stashed. Not caused here, not fixed here.

## The plan: steps 1 and 2 are DONE

Both landed on 2026-08-25, in the order the register said was load-bearing.

1. **`Get`/`Describe` now carry a call id** and are settled through per-call waiters, closing the
   silent bug where two concurrent host threads received each other's answers. An answer no waiter
   claims is dropped with a warning rather than handed to whoever is waiting - which also makes an
   engine too old to echo the id fail loudly instead of mis-delivering.
2. **Registered functions now run off the reader thread**, so a host function may call back into the
   engine while it is being invoked. The characterisation tests that pinned the hang were inverted
   rather than deleted, and two were added for the risks the fix introduces.

**Next is the windowing spike**, and its plan is written and committed:
[`docs/plans/2026-08-25-001-feat-streams-windowed-aggregation-plan.md`](../plans/2026-08-25-001-feat-streams-windowed-aggregation-plan.md).
Nine units, ordered so the two that need no new code run first. It has **not** been through
`ce-doc-review`. Two findings in it are worth knowing before reading: sinking a windowed table is
*refused* rather than encoded, because a Kafka topic key cannot carry the decomposed form and the
alternative is shipping Kafka's internal layout; and the hypothesis that a slow host breaches
`max.poll.interval.ms` was already refuted by the PoC, so that unit was re-aimed at the unclean-stop
path instead.

## The dimension decision, settled 2026-08-25

This section recorded an open question. It has been answered, and the answer reversed one of its own
proposals - so both halves are kept, because a reader who found only the conclusion could not tell
that the reasoning had been overturned rather than never written.

**Next is windowing.** It needs new surface rather than a new field: `fetch(key, from, to)` is a
range query where `Get` is point-only, a windowed key is composite against a flat `DataType`, and
stream time is an engine notion the host cannot see.

**Host-supplied serdes was proposed as the strongest remaining falsifier and withdrawn the same
day.** The engine uses `Serdes.ByteArray()` everywhere except where an operator mints a value it
created itself; the host serialises in its own language and hands over bytes, so nothing ever gives
the engine a host serde to call and the restore-path concern behind the proposal evaporates.
`next-kafka-streams-foreign-wrappers.md` had already settled it.

**Punctuators were rejected for this slot**, despite the deferred table calling them cheap: a
punctuator that cannot `forward()` is close to useless, and `forward()` needs one-in-many-out, which
is still open.

Full reasoning, including the caveat that dimension 1 was predicted to be fatal and was not:
[`streams-coupling-dimensions.md`](streams-coupling-dimensions.md), which **owns this decision**.

## Machine-local, not in the repo

The re-entrancy experiment's full report and its standalone reproduction live in this session's
scratchpad (`reentrancy-findings.md`, `reentrancy-concurrent-get-bug.py`). **The findings that
matter are already in the notes above and in commit `3f9191c75`'s message** - the scratchpad is
corroboration, not the record, and it will not survive.
