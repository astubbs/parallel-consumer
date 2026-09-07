#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# The inflight tag vocabulary FOR BASH - sourced by bin/check-inflight-tags.sh, the gate. It defines
# nothing itself: bin/lib/inflight-tags.mjs owns the sets, the impact order and every comment that
# explains them, and this file evals what `node bin/lib/inflight-tags.mjs --shell` prints, which is
# one `NAME="value value ..."` line per variable in SHELL_VARIABLES there.
#
# WHY THE NODE FILE OWNS IT. The vocabulary used to live here and be ported to Node for the session
# index, held equal by a parity self-test - two copies of a closed set, which is the drift this
# repository treats as a defect everywhere else. The index is the consumer that ORDERS by these
# values, so the file it imports is the natural owner; the gate only tests membership, and reads the
# same values through this wrapper. One source, no parity to keep.
#
# FAILS LOUDLY, NEVER EMPTY. A sourced library that cannot produce its variables must not return
# quietly: the gate would then compare every tag against empty sets and report every note invalid,
# or - with `set -u` off - nothing at all. Both a missing `node` and a failed render return non-zero
# with a message on stderr, so `. inflight-tags.sh || exit 1` in the gate stops there.
#
# shell-justified: the gate that sources this is bash, and a sourced file has to be the language of
# its sourcer; the whole point of this file is to be the smallest such bridge.

_inflight_tags_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! command -v node >/dev/null 2>&1; then
    printf 'inflight-tags.sh: node is not on PATH - bin/lib/inflight-tags.mjs owns the tag vocabulary and renders it for bash\n' >&2
    unset _inflight_tags_dir
    return 1 2>/dev/null || exit 1
fi

# Captured first, then evaled, so a render that fails part-way defines nothing rather than half.
if ! _inflight_tags_vars="$(node "$_inflight_tags_dir/inflight-tags.mjs" --shell)"; then
    printf 'inflight-tags.sh: `node %s/inflight-tags.mjs --shell` failed - no tag vocabulary defined\n' "$_inflight_tags_dir" >&2
    unset _inflight_tags_dir _inflight_tags_vars
    return 1 2>/dev/null || exit 1
fi

if ! eval "$_inflight_tags_vars"; then
    printf 'inflight-tags.sh: the rendered vocabulary did not eval as bash - no tag vocabulary defined\n' >&2
    unset _inflight_tags_dir _inflight_tags_vars
    return 1 2>/dev/null || exit 1
fi
unset _inflight_tags_dir _inflight_tags_vars
