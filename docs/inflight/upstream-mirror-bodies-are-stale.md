# The mirror bodies are wrong, and they are what people trust

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

Every open `upstream-mirror` issue carries a `## Fork status` section written when it was mirrored.
That section, not
[`upstream-map.yaml`](../../src/docs/development/upstream-map.yaml) and not
[`upstream-pr-analysis.adoc`](../../src/docs/development/upstream-pr-analysis.adoc), is where the
fork's only judgement about most upstream issues lives. A triage of ten of them checked those
sections against the tree. Most had drifted and several were wrong on the mechanism.

This matters more than a stale note usually would, because a mirror body is the artefact a reader
trusts *instead of* investigating. It reads as settled, it carries citations, and nothing about it
signals its age. An agent picking up one of these issues inherits its errors and builds on them.

The corrected text for each is drafted and ready to post, in the per-issue notes linked below.
**Nothing has been posted to any issue** - that is a separate, deliberate step.

Four further mirrors from the same sweep already had a fix in flight and were checked for a different
thing - whether the PR actually closes the issue. That is
[`pr-mirror-fixes-and-what-they-close.md`](pr-mirror-fixes-and-what-they-close.md).

## How they got this wrong

Not carelessness, and worth understanding rather than just fixing, because four of the five causes
will recur on the next mirror written.

**A single rename falsified one layer of every body at once.** The bodies cite `file:line`. The
`io.confluent` to `bz.stub` move changed every path and shifted every line in the tree, so every
citation in every mirror broke in one commit, silently, while still reading as valid. One of them
turned out to have been wrong from the day it was written - it named a method declaration rather
than the statement it claimed to cite, and nobody could have noticed.
[`docs/citations.md`](../citations.md) already forbids this form; the bodies predate the rule
holding.

**Nothing re-checks a body after it is written.** `upstream-map.yaml` carries a `last_checked` per
entry and has a sweep script behind it. An issue body has neither, so it is accurate on the day it
is written and decays untracked from then on. astubbs#175's body credits the correct PR for the day
it was written and the wrong one a fortnight later, because astubbs#204 merged in between.

**The summary was written from the summary.** The mirror header says "Summarised, not copied", which
is the right call for the upstream thread. But the `## Fork status` beneath it was then reasoned
from that summary rather than from the thread, and the discarded detail is exactly what a fork
assessment needs. astubbs#162's body misses that reporters hit the same warning *after* the fix
shipped, which is the single most important fact in its upstream thread and settles whether the
issue is closed. astubbs#175's body misses the failure the reporter himself called the bigger
problem.

**Plausible adjacency got recorded as attribution.** astubbs#175 credits astubbs#100 with fixing the
reporter's poll-thread death. astubbs#108 is the one that catches the exception in the reporter's
own logs. Both are rebalance-time commit fixes, so the plausible one was written down. Nothing in
the body's format distinguishes a claim that was verified from one that was inferred, so a guess and
a check are indistinguishable to the next reader - and the guesses are what failed.

**Upstream's own description was inherited as fact.** astubbs#241 restates upstream's account of
`commitOffsets`, which confluentinc#355 falsified in 2022 - before the mirror was written. Mirroring
carried the claim across without asking whether it was still true of our tree, or of any tree.

Underneath all five: **the body was authored as a mirroring artefact and is now read as a maintained
assessment.** It has no verified-on date, no separation of observed from inferred, and no trigger
that fires when the code beneath it moves. The cohort's own note
([`upstream-2023-sweep-manual-review.md`](upstream-2023-sweep-manual-review.md)) already admitted
the related version of this: verification depth was uneven, and the interesting ones got dug into
hardest.

## What needs correcting, per mirror

Each row's corrected text, with anchors and citations, is in the linked note.

| Mirror | What is wrong in its `## Fork status` | Corrected text in |
|---|---|---|
| astubbs#241 | **Premise false.** Claims one generic retry loop treating every transaction failure identically. confluentinc#355 replaced that in 2022; a coarse taxonomy has existed since | [`core-241-tx-commit-failure-taxonomy.md`](core-241-tx-commit-failure-taxonomy.md) |
| astubbs#189 | Understates it: omits that there is no retry ceiling, so innocent records re-run indefinitely rather than merely being delayed. Prescribes jitter as "the proper fix" when the real find is a half-built per-record seam. Predates the design that now exists | [`core-189-batch-failure-granularity.md`](core-189-batch-failure-granularity.md) |
| astubbs#181 | Never names the mechanism, which was a kafka-clients SASL callback rather than anything in this project. "Untested on Java 24" is now too strong. Reads as though astubbs#53 blocks it, and astubbs#53 answers a different question | [`deps-181-java-24-compatibility.md`](deps-181-java-24-compatibility.md) |
| astubbs#178 | "Not reproducible by inspection" is too broad - true only under the reporter's stated preconditions. The chaos-suite reasoning is stale: the suite does assert per-key concurrency now, and it is the epoch scoping that lets the case through | [`core-178-key-order-across-a-rebalance.md`](core-178-key-order-across-a-rebalance.md) |
| astubbs#175 | **Credits the wrong PR** for the reporter's failure. Treats confluentinc#818 and confluentinc#819 as upstream-only when this tree carries both. Omits the close-path failure entirely | [`upstream-175-sporadic-commit-timeouts.md`](upstream-175-sporadic-commit-timeouts.md) |
| astubbs#173 | **Contradicts our own README** on what the transactional mode guarantees. Names astubbs#29 as the closest fork work, when astubbs#29 skips the revocation commit by design and so moves this symptom the wrong way | [`upstream-173-revocation-duplicate-processing.md`](upstream-173-revocation-duplicate-processing.md) | <!-- post-merge: checked -->
| astubbs#163 | **False.** Says the poll is unguarded with only a blanket catch. A typed per-exception poll seam already exists and is load-bearing, which makes the fix far cheaper than the body implies. Quotes code that astubbs#204 has since changed | [`core-163-poll-path-has-no-error-seam.md`](core-163-poll-path-has-no-error-seam.md) |
| astubbs#162 | **Misses the headline**: reporters hit the same warning after the fix shipped, proving it partial. Understates what this tree already carries. Cites an unrelated open PR as the thing reworking the area | [`bug-162-offset-state-truncation.md`](bug-162-offset-state-truncation.md) |
| astubbs#161 | **Names the wrong thread.** Says the scheduler moves the user function off the control thread; it was never on the control thread. That changes the answer to the reporter's actual question | [`upstream-161-reactor-scheduler-rationale.md`](upstream-161-reactor-scheduler-rationale.md) |
| astubbs#139 | Points at the wrong surface: treats `subscribe` as the problem when it is the safest of the cross-thread methods, and never mentions the run-state machine, which is the unsafe one. Frames the whole issue as gated on astubbs#142 when most of it is not | [`core-139-public-api-thread-safety-contract.md`](core-139-public-api-thread-safety-contract.md) |

Line-number citations have drifted in every one of them. That is listed once here rather than ten
times above.

## What would stop it happening again

Unresolved, and worth a decision before the next mirror is written. Correcting these ten bodies
without this just resets the clock.

- **Give the `## Fork status` section a verified-on date**, the way map entries have `last_checked`.
  Without one there is no way to tell a fresh assessment from a three-year-old one, and no way for a
  sweep to find the stale ones.
- **Say which claims were checked against code and which were inferred.** The failures above are
  concentrated entirely in the inferred half, and nothing marks it.
- **Anchors, never line numbers**, per [`docs/citations.md`](../citations.md). This is the one cause
  with a mechanical fix.
- **A merged fork PR should mark every mirror it touches as needing a re-read.** astubbs#175 went
  wrong precisely here. `scripts/upstream-sweep.sh` is the natural home, since it already reasons
  about staleness.

## Delete when

The ten bodies above are corrected, and either the recurrence question has been decided and recorded
where the next mirror author will see it, or it has been explicitly declined.
