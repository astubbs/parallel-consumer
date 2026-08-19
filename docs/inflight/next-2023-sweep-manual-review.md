# Upstream closure audit - what still needs a human decision

<!-- inflight-priority: low -->

The 2023 administrative sweeps are mirrored and mapped (see
[`upstream-map.yaml`](../../src/docs/development/upstream-map.yaml), entries `sweep-2023-*`, fork
mirrors astubbs#227-254 under label `upstream-admin-closed`). `scripts/upstream-sweep.sh --audit`
now finds that class of closure with no time window, so the *detection* gap is closed.

What is left is judgement nobody has applied yet. Do not re-derive the lists here - run the audit.
This file records only what the command cannot tell you.

## 1. Two human PRs closed inside dependabot batches - are they lost work?

Found while checking whether the audit's bulk-day hits were real sweeps. Four of the six bulk PR days
are dependabot self-closing superseded bumps (the audit now filters bots for exactly this reason),
but two human PRs were sitting inside those batches:

| PR | Author | What | Closed by |
|---|---|---|---|
| confluentinc#508 | astubbs | `docs: "Back Pressure" notes and various Javadoc` | cprovencher, with a CLA-assistant "not signed" comment |
| confluentinc#650 | nvinayshetty | `fix maxConcurrency documentation in javadoc` | cprovencher, no comment at all |

Both are documentation, both plausibly still correct, and neither was evaluated on its merits as far
as the threads show. confluentinc#508 is our own writing. **Decision needed:** read both diffs and
decide whether to carry them into the fork. If yes they are cherry-picks, not mirrors - the content
is the value, not the thread.

Not yet checked: whether other *individually* closed human PRs were dismissed the same way. The
audit only surfaces bulk days (a documented blind spot - see [`docs/upstream.md`](../upstream.md),
"`--audit` - closures the window cannot see"), so a PR closed alone on a quiet day is still
invisible.

## 2. ~139 closed upstream issues are neither tracked nor mirrored

`scripts/upstream-sweep.sh --audit` prints the current list. Nobody has read it. Spot-checking says
most are genuinely completed - releases, real fixes, superseded work - so this is a triage job, not a
mirroring job, and it should NOT be mirrored wholesale the way the sweep cohort was: that cohort was
mirrored in full precisely because its closures were known to be meaningless, which is not true here.

**What the triage decides per issue:** was this closed because it was *done*, or because it was
*tidied*? `stateReason: COMPLETED` does not answer that - upstream marked all 28 swept issues
COMPLETED. The reliable signal is a linked merged PR or a closing comment describing a fix.

## 3. Mirror verification depth is uneven

Every "confirmed by reading the code" claim in astubbs#227-254 is real and specific, but the effort
behind them was not uniform - the ones that looked interesting got dug into hardest. Thinnest:
confluentinc#28 (tracing), confluentinc#40 (per-record transactions), confluentinc#49 (drain to plain
offsets), confluentinc#154 (proxy). For those four, absence was easy to establish (no dependency, no
module, no enum value) and little else was checked, so the *fork status* is sound while the
*design notes* are the least examined. Re-read them before planning any of that work.
