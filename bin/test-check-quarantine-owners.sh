#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Self-test for bin/check-quarantine-owners.sh.
#
# THE LOAD-BEARING ARM IS THE DEPTH INVARIANT: running this gate must leave the clone exactly as
# shallow as it found it. The gate reached an owner PR's base and merge preview with
# `git fetch --depth=1`, which writes the `shallow` file - and that file lives in the SHARED
# --git-common-dir, so one run truncated history for EVERY worktree of the clone. bin/check-all.sh
# sweeps `bin/check-*.sh`, so the mandated pre-push sweep was also an instruction to corrupt the
# clone, and the damage showed up in OTHER commands: empty merge-bases, ahead/behind counts in the
# hundreds, landed commits reported as "not an ancestor of master". Nothing went red.
#
# So the invariant is ASSERTED HERE rather than described in a comment there, and it is asserted
# from both sides - a full clone must stay full, and a clone that arrived shallow on purpose (CI
# checks out at depth 1) must not be deepened either. Verified red against the unfixed script before
# it landed: the full-clone arm reported `is-shallow=true`.
#
# HERMETIC BY CONSTRUCTION. The gate needs `gh` and the network; both are replaced - a fake `gh` on
# PATH answers the two queries it makes, and "origin" is a local file:// repository carrying real
# `refs/pull/N/merge` refs. So this runs offline, deterministically, and against the real gate rather
# than a copy of it.
set -uo pipefail

GATE="$(cd "$(dirname "$0")/.." && pwd)/bin/check-quarantine-owners.sh"
pass=0; fail=0

check() { # <name> <expected> <actual>
    if [ "$2" = "$3" ]; then printf 'ok:   %s\n' "$1"; pass=$((pass + 1))
    else printf 'FAIL: %s (expected %s, got %s)\n' "$1" "$2" "$3"; fail=$((fail + 1)); fi
}
check_contains() { # <name> <needle> <haystack>
    case "$3" in *"$2"*) printf 'ok:   %s\n' "$1"; pass=$((pass + 1)) ;;
        *) printf 'FAIL: %s (no "%s" in output)\n%s\n' "$1" "$2" "$3"; fail=$((fail + 1)) ;; esac
}

scratch="$(mktemp -d)"; trap 'rm -rf "$scratch"' EXIT
origin="$scratch/origin"

# ---------------------------------------------------------------------------- the fake remote
# Three commits, so "depth 1" is distinguishable from "all of it" by counting.
mkdir -p "$origin/src/test/java"
git init -q "$origin"
git -C "$origin" symbolic-ref HEAD refs/heads/master   # not `init -b`, which needs git >= 2.28
git -C "$origin" config user.email t@t
git -C "$origin" config user.name t
quarantined() { cat > "$origin/src/test/java/FlakyThingTest.java" <<'JAVA'
class FlakyThingTest {
    @Test
    @Quarantined(fixedBy = "PR astubbs#7", reason = "fixture")
    void someMethod() {}
}
JAVA
}
released() { cat > "$origin/src/test/java/FlakyThingTest.java" <<'JAVA'
class FlakyThingTest {
    @Test
    void someMethod() {}
}
JAVA
}
quarantined
for i in 1 2 3; do
    echo "$i" > "$origin/filler"
    git -C "$origin" add -A && git -C "$origin" commit -qm "c$i"
done

# refs/pull/7/merge REMOVES the quarantine - the loop closes, the gate must say OK.
released
git -C "$origin" add -A && git -C "$origin" commit -qm "pr7 releases the quarantine"
git -C "$origin" update-ref refs/pull/7/merge HEAD
git -C "$origin" reset -q --hard HEAD~1

# refs/pull/8/merge STILL CARRIES it - the gate must say no. A gate that can only say yes is not a
# gate, and "still works" is half the point of this fix.
echo touched > "$origin/filler2"
git -C "$origin" add -A && git -C "$origin" commit -qm "pr8 leaves the quarantine in place"
git -C "$origin" update-ref refs/pull/8/merge HEAD
git -C "$origin" reset -q --hard HEAD~1

# ---------------------------------------------------------------------------- the fake gh
fakebin="$scratch/fakebin"; mkdir -p "$fakebin"
cat > "$fakebin/gh" <<'GH'
#!/usr/bin/env bash
# Answers the two queries gh_query makes: `gh pr view <n> -R <repo> --json <field> -q <path>`.
n=""; field=""
while [ $# -gt 0 ]; do
    case "$1" in view) n="$2" ;; --json) field="$2" ;; esac
    shift
done
case "$field" in
    state)
        # PR 9 exists only to be interrupted: it blocks until the caller kills the gate. `exec`, so
        # this process BECOMES sleep - a forked child would keep the command substitution's pipe
        # open after its parent was killed, and the gate would stay blocked anyway.
        # `exec`, so this process BECOMES sleep and keeps this pid: a forked child would hold the
        # command substitution's pipe open after its parent died, and outlive the test as an orphan.
        # The pid goes in the marker so the caller can reap it rather than wait out the sleep.
        if [ "$n" = "9" ]; then echo $$ > "${FAKE_GH_BLOCKED_MARKER:-/dev/null}"; exec sleep 30; fi
        echo OPEN ;;
    baseRefName) echo master ;;
    *) exit 1 ;;
esac
GH
chmod +x "$fakebin/gh"

registry() { # <workdir> <entries...>
    local w="$1"; shift
    mkdir -p "$w/docs"
    { echo "# Quarantined tests"; echo; printf '%s\n' "$@"; } > "$w/docs/quarantined-tests.md"
}
run_gate() { # <workdir> ; echoes the gate's output, TMPDIR pinned so leaks are observable
    local w="$1"
    rm -rf "$scratch/tmp"; mkdir -p "$scratch/tmp"
    ( cd "$w" && PATH="$fakebin:$PATH" TMPDIR="$scratch/tmp" QUARANTINE_CHECK_ROOT="$w" \
        bash "$GATE" 2>&1 )
}

# ============================================================ 1. a FULL clone must stay full
full="$scratch/full"
git clone -q "file://$origin" "$full"
registry "$full" "- [ ] \`FlakyThingTest.someMethod\` - fixture entry." "  Owner: PR astubbs#7"
out="$(run_gate "$full")"

check "a full clone is NOT left shallow by the gate" \
    "false" "$(git -C "$full" rev-parse --is-shallow-repository)"
check "no shallow file is written into the shared git dir" \
    "absent" "$([ -e "$full/.git/shallow" ] && echo present || echo absent)"
check "the clone still has its whole history" \
    "3" "$(git -C "$full" rev-list --count HEAD)"
check "the scratch fetch dir is cleaned up" \
    "" "$(ls -A "$scratch/tmp" 2>/dev/null)"

# THE GATE MUST STILL CHECK. Removing the side effect by removing the check would pass every
# assertion above and be a worse script than the one it replaced.
check_contains "the merge preview that removes the quarantine reads as OK" \
    "OK: FlakyThingTest.someMethod owner PR #7 is open and its merge result removes the quarantine" "$out"  # issue-refs: exempt - the gate's own output format, not a reference to anything

# ============================================================ 2. and it must still be able to say no
full2="$scratch/full2"
git clone -q "file://$origin" "$full2"
registry "$full2" "- [ ] \`FlakyThingTest.someMethod\` - fixture entry." "  Owner: PR astubbs#8"
out2="$(run_gate "$full2")"
check_contains "a merge preview that keeps the quarantine is reported" \
    "does NOT yet remove the quarantine" "$out2"
check "saying no does not shallow the clone either" \
    "false" "$(git -C "$full2" rev-parse --is-shallow-repository)"

# ============================================================ 3. a DELIBERATELY shallow clone
# CI checks out at depth 1 on purpose. Unshallowing it would make every job slower for no benefit,
# so the fix must not "repair" what it did not break.
shallow="$scratch/shallow"
git clone -q --depth=1 "file://$origin" "$shallow"
registry "$shallow" "- [ ] \`FlakyThingTest.someMethod\` - fixture entry." "  Owner: PR astubbs#7"
out3="$(run_gate "$shallow")"
check "a deliberately shallow clone stays shallow" \
    "true" "$(git -C "$shallow" rev-parse --is-shallow-repository)"
check "a deliberately shallow clone is not deepened" \
    "1" "$(git -C "$shallow" rev-list --count HEAD)"
check_contains "the gate still verifies from a shallow clone" \
    "owner PR #7" "$out3"  # issue-refs: exempt - matches the gate's printed output, not a real PR

# ============================================================ 4. interrupted mid-run
# The first entry completes, so the scratch fetch dir exists; the second blocks in `gh` until we
# kill the gate. The clone must be untouched, because it was never the fetch target.
killable="$scratch/killable"
git clone -q "file://$origin" "$killable"
registry "$killable" \
    "- [ ] \`FlakyThingTest.someMethod\` - fixture entry." \
    "  Owner: PR astubbs#7" \
    "" \
    "- [ ] \`OtherThingTest.blocks\` - fixture entry that blocks." \
    "  Owner: PR astubbs#9"
marker="$scratch/blocked"
rm -rf "$scratch/tmp"; mkdir -p "$scratch/tmp"
# `exec`, so the backgrounded subshell BECOMES the gate and $! is the gate's own pid. Without it,
# $! is the wrapper and the signal lands on the wrong process - which is how this arm first passed
# its kill and then failed its assertion.
( cd "$killable" || exit 1
  export PATH="$fakebin:$PATH" TMPDIR="$scratch/tmp" QUARANTINE_CHECK_ROOT="$killable"
  export FAKE_GH_BLOCKED_MARKER="$marker"
  exec bash "$GATE" >/dev/null 2>&1 ) &
gate_pid=$!
# BOUNDED WAIT, and the bound is a failure, not a timeout to sleep through. `-s`, not `-e`: an
# existing but still-empty marker would hand the kill below an empty pid.
waited=0
while [ ! -s "$marker" ] && [ "$waited" -lt 200 ]; do sleep 0.1; waited=$((waited + 1)); done
check "the gate reached the blocking entry" "yes" "$([ -s "$marker" ] && echo yes || echo no)"
# The gate is parked in a foreground child, and bash defers a trap until that child returns - so
# everything below it has to go too, or the TERM is never serviced and this arm just times out.
# The blocked `gh` is a GRANDchild (command substitution forks its own subshell), which is why its
# pid is passed out through the marker rather than reached with `pkill -P`.
kill -TERM "$(cat "$marker")" 2>/dev/null
pkill -TERM -P "$gate_pid" 2>/dev/null
kill -TERM "$gate_pid" 2>/dev/null
wait "$gate_pid" 2>/dev/null

# THE CLONE IS THE ASSERTION THAT MATTERS, and it is instantaneous: the clone was never the fetch
# target, so no ordering of the teardown can leave it shallow.
check "an interrupted gate leaves the clone unshallowed" \
    "false" "$(git -C "$killable" rev-parse --is-shallow-repository)"

# CLEANUP IS THE SECOND ASSERTION, and it is instantaneous on purpose. It was flaky at three passes
# in five while the gate reached its EXIT trap by `exit`-ing from the signal handler, and the fix
# for that is in the gate, not a poll here - a bounded wait would have hidden a real defect behind
# a timeout. The handlers now do their own cleanup, so by the time `wait` returns it has happened.
check "an interrupted gate cleans up its scratch dir" \
    "" "$(ls -A "$scratch/tmp" 2>/dev/null)"

printf '\n%s passed, %s failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
