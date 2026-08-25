#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Self-test for bin/check-pr-analysis-surfaces.sh.
#
# The whole value of that script is one distinction: a finding ON A LINE THE PR WROTE versus a
# finding merely IN A FILE THE PR OPENED. Get that wrong in either direction and the tool is useless
# - too loose and every small PR to a big class inherits dozens of findings and the report gets
# ignored; too tight and it reports "none" over a defect the author just introduced, which is the
# false green this whole branch exists to remove.
#
# So the arms are paired: the SAME annotation, at the SAME line, against a diff that either did or
# did not add that line. Nothing but the diff changes between them.
#
# `gh` is faked on PATH rather than mocked inside the script, so the subject runs completely
# unmodified - no test-only branch inside it that could drift from the real path.

set -euo pipefail

subject="$(cd "$(dirname "$0")" && pwd)/check-pr-analysis-surfaces.sh"
pass=0
fail=0

# Builds a fake `gh` answering exactly the calls the subject makes.
# $1 fixture dir, $2 the line number the diff ADDS, $3 the line number the annotation is ON.
make_gh() {
    local dir="$1" added_line="$2" ann_line="$3" runs="${4:-1}"
    mkdir -p "$dir/bin"
    cat > "$dir/bin/gh" <<GHEOF
#!/usr/bin/env bash
set -eu
case "\$*" in
    *"--json number"*)      echo 999 ;;
    *"--json headRefOid"*)  echo "abc123def456" ;;
    *"--json files"*)       echo "src/main/java/Foo.java" ;;
    *"pr diff"*)
        printf '%s\n' "diff --git a/src/main/java/Foo.java b/src/main/java/Foo.java"
        printf '%s\n' "--- a/src/main/java/Foo.java"
        printf '%s\n' "+++ b/src/main/java/Foo.java"
        printf '%s\n' "@@ -${added_line},0 +${added_line},1 @@"
        printf '%s\n' "+        int addedByThisPr = 1;"
        ;;
    *"commits/"*"check-runs"*)
        # \$runs check runs, each reporting the SAME finding - the duplication a javac problem
        # matcher produces across every compiling job.
        printf '{"check_runs":['
        for i in \$(seq 1 ${runs}); do
            [ "\$i" = 1 ] || printf ','
            printf '{"id":%s,"name":"job%s","html_url":"http://x","output":{"annotations_count":1}}' "\$i" "\$i"
        done
        printf ']}'
        ;;
    *"check-runs/"*"annotations"*page=1*)
        printf '[{"path":"src/main/java/Foo.java","start_line":${ann_line},"message":"the one finding"}]'
        ;;
    *"annotations"*)  printf '[]' ;;
    *"issues/"*"comments"*) printf '' ;;
    *) printf '' ;;
esac
GHEOF
    chmod +x "$dir/bin/gh"
}

run_subject() {
    local dir="$1" out rc
    set +e
    out="$(PATH="$dir/bin:$PATH" bash "$subject" 999 2>&1)"
    rc=$?
    set -e
    printf '%s\n---RC:%s\n' "$out" "$rc"
}

assert_arm() {
    local name="$1" want_rc="$2" want="$3" result="$4"
    local rc; rc="$(sed -n 's/^---RC:\(.*\)$/\1/p' <<< "$result")"
    if [ "$rc" = "$want_rc" ] && grep -qF "$want" <<< "$result"; then
        printf 'ok:   %s\n' "$name"; pass=$((pass + 1))
    else
        printf 'FAIL: %s (exit %s, wanted %s, looking for "%s")\n%s\n' "$name" "$rc" "$want_rc" "$want" "$result"
        fail=$((fail + 1))
    fi
}

echo "=== the distinction the whole script exists for ==="

# --- RED: the annotation sits on a line the diff ADDED. That is the author's own defect. ----------
d1="$(mktemp -d)"; make_gh "$d1" 42 42
r1="$(run_subject "$d1")"
assert_arm "red: a finding on a line THIS PR ADDED exits 1" 1 "ON LINES THIS PR WROTE" "$r1"
assert_arm "red: ...and it is counted as the author's, not inherited" 1 "1 distinct finding(s)" "$r1"
rm -rf "$d1"

# --- GREEN NEAR-MISS: same annotation, same file, one line away from the diff ---------------------
# If this went red too, the script would be reporting "every finding in a file you opened", which is
# the failure mode that gets a report ignored rather than read.
d2="$(mktemp -d)"; make_gh "$d2" 42 900
r2="$(run_subject "$d2")"
assert_arm "green near-miss: the same finding one line outside the diff exits 0" 0 "inherited" "$r2"
assert_arm "green near-miss: ...and the author's section is empty" 0 "none" "$r2"
rm -rf "$d2"

echo
echo "=== duplication across jobs must not inflate the count ==="

# One javac warning is annotated once per compiling job. Counting those separately would report
# eight findings where there is one, and bury the distinct ones.
d3="$(mktemp -d)"; make_gh "$d3" 42 42 4
r3="$(run_subject "$d3")"
assert_arm "4 check runs reporting one finding is ONE distinct finding" 1 "1 distinct finding(s)" "$r3"
assert_arm "...and every reporting job is still named" 1 "job1, job2, job3, job4" "$r3"
rm -rf "$d3"

echo
echo "=== cannot-run must never read as clean ==="

# --- RED CONTROL: no gh at all. The contract bin/racerd-test.sh and ci-mutation-test.sh hold. -----
# The PATH keeps /usr/bin:/bin - enough for bash and coreutils - and `gh` is not installed there on
# macOS or on a hosted Linux runner, so this removes gh WITHOUT removing the shell. An empty PATH
# made this arm exit 127 on "bash: command not found", which is a pass for entirely the wrong reason
# and would have kept passing if the subject had stopped checking for gh at all.
d4="$(mktemp -d)"; mkdir -p "$d4/bin"
if PATH=/usr/bin:/bin command -v gh > /dev/null 2>&1; then
    printf 'skip: cannot stage an absent gh - it is installed in /usr/bin or /bin on this machine\n'
else
    set +e
    out4="$(PATH="$d4/bin:/usr/bin:/bin" bash "$subject" 999 2>&1)"; rc4=$?
    set -e
    if [ "$rc4" = "2" ] && grep -qF "CANNOT RUN" <<< "$out4"; then
        printf 'ok:   red: absent gh exits 2, not 0\n'; pass=$((pass + 1))
    else
        printf 'FAIL: absent gh exited %s (wanted 2)\n%s\n' "$rc4" "$out4"; fail=$((fail + 1))
    fi
fi
rm -rf "$d4"

printf '\n%s passed, %s failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
