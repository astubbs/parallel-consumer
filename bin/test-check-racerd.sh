#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Self-test for bin/racerd-test.sh.
#
# WHY IT EXISTS, and why it is late. The other two gates this branch added shipped with self-tests;
# this one did not, and it is the only one of the three that reached CI red. A code review found two
# defects in it that neither of its siblings had: the classpath was resolved over the whole reactor
# so the file left behind was the LAST module's, and Infer's own exit status was captured and never
# checked. Both are the branch's own defect class - an analysis that ran against something other than
# what was intended, reporting a number that looked like a measurement. The gate with no test was the
# gate that had them.
#
# WHAT IT COVERS, AND WHAT IT DOES NOT - stated because a partial self-test that reads as complete is
# the same failure one level up. The two preflight guards run before any Maven or Infer work, so they
# are cheap to exercise and are covered here with red arms and green near-misses. The ceiling
# arithmetic and the new exit-status check sit AFTER a full analysis, and the script is not currently
# structured to let a fake Infer reach them - it resolves a real classpath and enumerates real
# sources first. Covering those needs the verdict logic extracted into a function this test can call
# directly, which is a refactor and not a test. Until then those arms are exercised by hand, and this
# header is the record that they are not exercised here.

set -euo pipefail

pass=0
fail=0

repo_root="$(git rev-parse --show-toplevel)"

# $1 name, $2 expected exit, $3 substring the output must contain, then env assignments
assert_guard() {
    local name="$1" want_rc="$2" want="$3"; shift 3
    local out rc
    set +e
    out="$(cd "$repo_root" && env "$@" bin/racerd-test.sh 2>&1)"
    rc=$?
    set -e
    if [ "$rc" != "$want_rc" ]; then
        printf 'FAIL: %s (expected exit %s, got %s)\n%s\n' "$name" "$want_rc" "$rc" "$out"
        fail=$((fail + 1))
    elif ! grep -qF "$want" <<< "$out"; then
        # Exit code alone is not proof. Both guards return 2, so without this a script that died for
        # an unrelated reason would score the arm it was never asked about.
        printf 'FAIL: %s (exit %s was right, but never said why - wanted %s)\n%s\n' \
            "$name" "$rc" "$want" "$out"
        fail=$((fail + 1))
    else
        printf 'ok:   %s\n' "$name"
        pass=$((pass + 1))
    fi
}

# --- RED: a missing toolchain must be "cannot run", never "no races found" -----------------------
assert_guard "red: absent Infer exits 2, not 0" 2 "CANNOT RUN" \
    INFER_BIN=/nonexistent/infer

# --- RED: a missing JDK is also cannot-run, and says which knob fixes it -------------------------
assert_guard "red: absent JDK 17 exits 2 and names RACERD_JDK" 2 "RACERD_JDK" \
    INFER_BIN=/bin/echo RACERD_JDK=/nonexistent/jdk

# --- GREEN NEAR-MISS: the JDK guard must not fire when the JDK is fine ---------------------------
# Same invocation as the arm above with only RACERD_JDK corrected. If this went red the second arm
# would be proving nothing - it would be failing on the absent Infer it shares, not on the JDK.
#
# RESOLVING THE JDK IS THE POINT OF THIS ARM, so it may not quietly give up. It used to try exactly
# one path - a developer's own SDKMAN install - and `return` when it was absent, printing `skip` and
# incrementing neither counter. `repo-hygiene.yml` runs this file on ubuntu-latest with no
# `setup-java` and no `RACERD_JDK`, where that path never exists, so the arm proving a good JDK gets
# PAST the guard has never once run in CI while the job reported success. A green tick over an arm
# that did not execute is the precise defect this whole branch is about, sitting inside its own
# self-test. Found by an independent review.
#
# So: try the explicit knob, then the runner's own JDK, then the developer path. Any javac will do -
# the guard under test only checks that `$JDK/bin/javac` is executable, not which version it is.
# And when nothing resolves, a local developer may skip, but CI must FAIL: on a hosted runner an
# unresolvable JDK means the fixture is wrong, not that the check is unnecessary.
jdk_near_miss() {
    local out rc real_jdk=""
    for candidate in "${RACERD_JDK:-}" "${JAVA_HOME:-}" "$HOME/.sdkman/candidates/java/17.0.18-tem"; do
        if [ -n "$candidate" ] && [ -x "$candidate/bin/javac" ]; then real_jdk="$candidate"; break; fi
    done
    if [ -z "$real_jdk" ]; then
        if [ -n "${CI:-}" ]; then
            printf 'FAIL: green near-miss found no JDK on a CI runner - set RACERD_JDK or add setup-java.\n'
            printf '      A skipped arm is not a passed arm, and this one is the only proof the JDK guard\n'
            printf '      does not fire on a valid JDK.\n'
            fail=$((fail + 1))
            return
        fi
        printf 'skip: green near-miss needs a JDK (tried RACERD_JDK, JAVA_HOME, the SDKMAN path)\n'
        return
    fi
    set +e
    out="$(cd "$repo_root" && INFER_BIN=/nonexistent/infer RACERD_JDK="$real_jdk" bin/racerd-test.sh 2>&1)"
    rc=$?
    set -e
    if [ "$rc" = "2" ] && grep -qF "Infer is not installed" <<< "$out"; then
        printf 'ok:   green near-miss: a good JDK leaves only the Infer guard firing\n'
        pass=$((pass + 1))
    else
        printf 'FAIL: green near-miss (exit %s, expected the Infer guard alone)\n%s\n' "$rc" "$out"
        fail=$((fail + 1))
    fi
}
jdk_near_miss

# --- The ratchet's four arms, exercised against a canned report so no Infer run is needed --------
# The decisive one is the SAME-COUNT SWAP: fix one race, introduce another, and the total is 13 both
# times. The count ceiling this ratchet replaced passed that, which is what an independent review
# caught - so an identity ratchet that is never machine-checked would be the same trap one level up.
#
# This block used to be a lie. It required RACERD_TEST_REPORT, which nothing ever set, so in CI it
# printed `skip`; and when a report WAS supplied it printed `ok:` and incremented the pass counter
# WITHOUT RUNNING ANY ARM, under this same comment claiming four were exercised. A green tick over an
# assertion that does not exist is the exact defect this branch is about. The arms below are real,
# they need no Infer run (bin/racerd-test.sh grew RACERD_DRY_RUN_REPORT for it), and they therefore
# run everywhere - locally and on every hosted runner.
#
# The fixtures are built here rather than checked in: they must stay in step with the identity format
# the ratchet computes (`<count> <bug_type> <Class.method>`), and a checked-in fixture drifts from it
# silently. Two known identities is enough to express every arm.
ratchet_arms() {
    local tmp report known out rc
    tmp="$(mktemp -d)"

    # A minimal Infer report.json: the ratchet reads bug_type and procedure, nothing else.
    _report() {
        python3 -c "
import json,sys
out=[]
for spec in sys.argv[2:]:
    bug,cls,meth = spec.split(':')
    out.append({'bug_type':bug,'procedure':'void %s.%s(java.lang.Object)' % (cls,meth),
                'file':'src/main/java/%s.java' % cls,'line':42,'qualifier':'canned fixture'})
json.dump(out, open(sys.argv[1],'w'))
" "$1" "${@:2}"
    }

    report="$tmp/report.json"
    known="$tmp/known.txt"
    printf '1 THREAD_SAFETY_VIOLATION Alpha.methodOne\n1 THREAD_SAFETY_VIOLATION Beta.methodTwo\n' > "$known"

    _arm() {
        local name="$1" want_rc="$2" want="$3"; shift 3
        _report "$report" "$@"
        set +e
        out="$(cd "$repo_root" && RACERD_DRY_RUN_REPORT="$report" RACERD_KNOWN="$known" bin/racerd-test.sh 2>&1)"
        rc=$?
        set -e
        if [ "$rc" = "$want_rc" ] && { [ -z "$want" ] || grep -qF "$want" <<< "$out"; }; then
            printf 'ok:   %s\n' "$name"
            pass=$((pass + 1))
        else
            printf 'FAIL: %s (exit %s, wanted %s)\n%s\n' "$name" "$rc" "$want_rc" "$out"
            fail=$((fail + 1))
        fi
    }

    # GREEN BASELINE: exactly the known set, so the ratchet must pass.
    _arm "ratchet: the known set exactly reproduced passes" 0 "all 2 finding(s) are known" \
        THREAD_SAFETY_VIOLATION:Alpha:methodOne THREAD_SAFETY_VIOLATION:Beta:methodTwo

    # RED: an identity that is not in the set means a race was introduced.
    _arm "ratchet: a NEW identity fails" 1 "NEW race(s)" \
        THREAD_SAFETY_VIOLATION:Alpha:methodOne THREAD_SAFETY_VIOLATION:Beta:methodTwo \
        THREAD_SAFETY_VIOLATION:Gamma:methodThree

    # RED: a known identity that stopped firing means somebody fixed a race without ratcheting.
    # Fails on purpose - a set nobody shrinks stops meaning anything.
    _arm "ratchet: a known identity that no longer fires fails" 1 "no longer fire" \
        THREAD_SAFETY_VIOLATION:Alpha:methodOne

    # THE DECISIVE ARM: one known identity swapped for one unknown. The COUNT IS UNCHANGED at 2, so a
    # ceiling passes this and an identity set must not. This is the arm the whole rewrite was for.
    _arm "ratchet: a same-count SWAP fails (the arm a ceiling passes)" 1 "NEW race(s)" \
        THREAD_SAFETY_VIOLATION:Alpha:methodOne THREAD_SAFETY_VIOLATION:Delta:methodFour

    rm -rf "$tmp"
}
ratchet_arms

printf '\n%s passed, %s failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
