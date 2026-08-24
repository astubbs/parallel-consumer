#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# THE single home of the machine-readable inflight tag sets - the same move as
# bin/lib/quarantine-common.sh: source this, do not copy from it. docs/inflight/AGENTS.md owns what
# each value MEANS; this file owns which values exist, for both consumers:
#
#   - bin/check-inflight-tags.sh          - the gate: rejects a tag outside these sets
#   - .claude/hooks/inject-recorded-knowledge.sh - the session index: groups open notes by them
#
# The two used to carry private copies, each annotated "these WILL drift" - and the failure mode of
# drift is the worst one this system has: a value the gate accepts but the index cannot place files
# a note into "unmatched", or loses it entirely. One source makes that class impossible.
#
# ORDER IS LOAD-BEARING for the index, not the gate: within each partition the values are listed by
# cost of not knowing, signal-integrity classes first (you cannot judge the code through instruments
# that lie, so `misdirection` outranks `blind-spot` and both outrank any product defect), and the
# index emits its groups in exactly this order.
#
# THE PARTITION IS THE POINT - bug impacts and task impacts are separate sets, not one flat list,
# because the index groups them separately: a `bug` carrying `release-gate` once passed a flat-set
# gate and then appeared under "unmatched" in the index.
#
# Adding a value: add it here AND describe it in docs/inflight/AGENTS.md in the same commit, and say
# why the existing values do not fit - do not invent one in a note and hope.

# A REGISTER is consulted, never completed - a ranked backlog, a collision list. It has no done
# state, so filing it as a `task` implied a discrete action someone could finish and sorted it among
# things waiting to be done, when it is the thing you READ to decide what to do next. Surfaced in its
# own section at the top of the session index rather than among open work.
INFLIGHT_TYPES="bug feature task register"
INFLIGHT_BUG_IMPACTS="misdirection blind-spot crash data-loss stall security config-lie reliability throughput"
INFLIGHT_TASK_IMPACTS="release-gate coordination stranded-work ci test-debt refactor process deps-debt security reliability"

# A FEATURE MAY CARRY AN IMPACT, and should whenever it addresses one. The point of the tag is that
# work falls out in priority order, not that it is filed under the correct part of speech: a
# commit-failure seam whose motivation is PC shutting down is `feature` + `crash`, and tagging it
# impact-less buries it among cosmetic features. Optional, because a genuinely new capability with no
# problem behind it has an opportunity rather than a consequence.
INFLIGHT_FEATURE_IMPACTS="$INFLIGHT_BUG_IMPACTS $INFLIGHT_TASK_IMPACTS"
# A register may carry one too - a collision list whose cost of being unread is a collision.
INFLIGHT_REGISTER_IMPACTS="$INFLIGHT_BUG_IMPACTS $INFLIGHT_TASK_IMPACTS"

# THE ORDER THE SESSION INDEX PRESENTS THEM IN, across every type - because a feature that prevents a
# crash must appear beside the crashes, not after them. Signal integrity first (you cannot judge the
# code through instruments that lie), then what kills, then what corrupts, then what stops, then what
# is merely owed.
INFLIGHT_IMPACT_ORDER="misdirection blind-spot crash data-loss stall security config-lie reliability throughput release-gate coordination stranded-work ci test-debt refactor process deps-debt"
