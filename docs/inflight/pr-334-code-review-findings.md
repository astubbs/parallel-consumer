# astubbs#334's code-review findings: 24 actionable, none yet fixed

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

The 2026-08-25 multi-agent review of this PR's pre-spike content (nine reviewers, eight findings
independently validated, verdict **Ready with fixes**) produced the queue below. **Nothing in it
has been fixed** - the review was report-only, and this note is its durable record; the full
artifacts (per-reviewer JSON, evidence, suggested fixes) are machine-local at
`/tmp/compound-engineering-1000/ce-code-review/20260825-031225-cf45e012/` and will not survive
that machine, so the suggested-fix detail worth keeping is the one-line titles plus the grouping
here.
<!-- file-refs: N/A - the /tmp run-artifact path above is machine-local by nature, cited as provenance -->

**The one decision gate (owner's call, blocks its group): the wire error contract.** Builder-call
and describe refusals currently surface as a session-level `Fault` with no call id; the engine
answers-and-continues while the Python client treats any `Fault` as permanently fatal - so one
refused call bricks a client the engine believes is healthy (findings 7, 21, 25). Either refusals
become per-call in-band answers (`call_id` + error, as `Get` already does; `Fault` reserved for
genuinely fatal states) or they become fatal on BOTH ends. Reviewers recommend in-band. **Owner,
2026-08-25: decision deferred to merge-prep** - the group stays blocked until then, taken up when
this queue is worked.

**Fix order recommended at review time:** the error-contract group above; then the
shutdown-lifecycle group (3, 14, 13 - nothing releases waiters on teardown, both sides + the
transport half-close race); then the two guards (5 - the registry test cannot detect crossed
answers; 6 - a failing invocation silently kills the topology); then docs/PR-narrative drift
(1, 4, 2, 10, 17, 9, 8); duplication follow-ups at leisure (22, 23, 24, 15, 16, 20, 26).
Finding 4 (the PR body) and 21 are human-owned; the rest are `downstream-resolver`.

| # | Sev | Where | Finding | Conf | Reviewer(s) |
|---|---|---|---|---|---|
| 1 | P1 | `next-kafka-streams-foreign-wrappers.md:233` | Inflight note contradicts itself about what the PoC covers | 100 | maintainability |
| 2 | P1 | `pr-334-handoff.md:6` | Handoff note carries the forbidden 'delete when merges' marker | 100 | project-standards |
| 3 | P1 | `_session.py:404` | close() leaves pending waiters to hang out the full timeout | 100 | testing,reliability,adversarial |
| 4 | P1 | `ForeignJoiner.java:21` | PR body denies workstreams the PR actually ships | 100 | maintainability |
| 5 | P1 | `InvocationRegistryTest.java:83` | Registry concurrency test cannot detect crossed-waiter mis-delivery | 100 | testing |
| 6 | P1 | `StreamsMain.java:103` | One failing invocation silently shuts down the whole topology | 75 | reliability |
| 7 | P1 | `StreamsSessionService.java:95` | Builder-call and describe errors bypass the wire's correlation contract | 75 | api-contract |
| 8 | P2 | `CONCEPTS.md:38` | CONCEPTS.md Bundling entry embeds file paths | 100 | project-standards |
| 9 | P2 | `bug-proto-breaking-gate-cannot-run-on-macos.md:4` | New bug note tagged with a task-only impact, gate rejects it | 100 | project-standards |
| 10 | P2 | `pr-334-handoff.md:106` | Handoff note writes down branch divergence a command answers | 100 | project-standards |
| 11 | P2 | `_session.py:559` | FunctionKind lacks UNKNOWN degradation; unknown invocation kind stalls the stream thread | 100 | testing,maintainability |
| 12 | P2 | `_session.py:592` | User function returning None or non-bytes is masked as empty bytes | 100 | correctness,reliability,adversarial |
| 13 | P2 | `_transport.py:63` | Transport close races half-close against channel cancellation | 100 | correctness,reliability,adversarial |
| 14 | P2 | `StreamsSessionService.java:326` | Session teardown never fails pending invocations; stream threads wait out 30s | 100 | correctness,reliability,adversarial |
| 15 | P2 | `TopologyAssembler.java:166` | Orphaned and duplicated javadoc blocks in TopologyAssembler | 100 | maintainability |
| 16 | P2 | `streams.proto:307` | Proto aggregate-field comment documents superseded presence-based dispatch | 100 | maintainability |
| 17 | P2 | `next-kafka-streams-foreign-wrappers.md:4` | PR touches inflight notes without fixing their invalid or missing tags | 75 | project-standards |
| 18 | P2 | `run.sh:93` | Streams demo arm has no output-suite assertion or contract entry | 75 | project-standards |
| 19 | P2 | `streams_demo.py:231` | Reused --topic lets the verifier certify a run with stale sink data | 75 | adversarial |
| 20 | P2 | `_session.py:578` | Second-argument selection escapes the _leading_argument table | 75 | maintainability |
| 21 | P2 | `_session.py:610` | One refused call permanently poisons the client session the engine keeps alive | 75 | adversarial |
| 22 | P2 | `ForeignJoiner.java:28` | Three Foreign* bridges duplicate identical plumbing; a fourth is planned | 75 | maintainability |
| 23 | P2 | `ForeignValueMapper.java:28` | Bot-flagged new clones neither removed nor dismissed | 75 | previous-comments |
| 24 | P2 | `StreamsSessionService.java:244` | Query encode duplicates the assembler's canonical DataType-to-serde mapping | 75 | maintainability |
| 25 | P2 | `streams.proto:224` | Describe silently forecloses further builder calls; wire and client never say so | 75 | api-contract |
| 26 | P3 | `TopologyAssembler.java:115` | mappers factory not null-checked, unlike its two siblings | 100 | maintainability |
Also carried from that review, unactioned: two missing `docs/solutions/` captures the learnings
pass flagged (close-versus-break; a point sample cannot certify absence of a transient), and the
two duplication-report bot comments awaiting remove-or-dismiss.

Delete this note in the change that empties the queue, or shrink it to what remains.
