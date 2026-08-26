#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Self-test for bin/check-shell-hazards.sh.
#
# The negative arms carry the weight. A hazard scanner that also flags the COMMENTS warning about
# the hazard is unusable in this repo, because those comments are how the knowledge survived before
# the gate existed - and a gate people mute protects nothing. So "documentation is not a use" and
# "a marked use is allowed" are tested as hard as the detection itself.
set -uo pipefail
# hazard-ok-file: every fixture below is literally a hazard - that is what is being tested. Stated
# DELIBERATELY, because this file was already exempt by accident: the arm that tests the
# `hazard-ok-file:` marker embeds that string in its fixture body, which exempted the real file as a
# side effect. Deleting that one arm would have silently started flagging every other arm.
GATE="$(cd "$(dirname "$0")/.." && pwd)/bin/check-shell-hazards.sh"
pass=0; fail=0
scratch="$(mktemp -d)"; trap 'rm -rf "$scratch"' EXIT

arm() { # <name> <expect fire|clean> <script-body>
    local name="$1" expect="$2" body="$3" d out rc got
    d="$scratch/$(echo "$name" | tr -c 'a-zA-Z0-9' '_')"; mkdir -p "$d"
    printf '#!/usr/bin/env bash\n%s\n' "$body" > "$d/subject.sh"
    out="$(bash "$GATE" "$d" 2>&1)"; rc=$?
    got=clean; [ "$rc" -ne 0 ] && got=fire
    if [ "$got" = "$expect" ]; then printf 'ok:   %s\n' "$name"; pass=$((pass + 1))
    else printf 'FAIL: %s (expected %s, got %s, rc=%s)\n%s\n' "$name" "$expect" "$got" "$rc" "$out"; fail=$((fail + 1)); fi
}

# DETECTION - each of these answers differently on the other platform rather than erroring.
arm "bare sed -i is caught"          fire  'sed -i "s/a/b/" f'
arm "stat -c is caught"              fire  'x=$(stat -c %Y f)'
arm "stat -f is caught"              fire  'x=$(stat -f %m f)'
arm "date -d is caught"              fire  'date -d "1 day ago"'
arm "readlink -f is caught"          fire  'p=$(readlink -f .)'
arm "grep -P is caught"              fire  'grep -P "\d" f'
arm "sort -V is caught"              fire  'sort -V < f'
arm "base64 -w (spaced) is caught"   fire  'base64 -w 0 < f'
arm "sed -r is caught"               fire  'sed -r "s/a/b/" f'
# ATTACHED-VALUE FORMS, and in both cases the attached spelling is the GNU-only one - so a pattern
# that only matched the spaced form would let the dangerous spelling through.
arm "sed -i.bak (attached) is caught" fire  'sed -i.bak "s/a/b/" f'
arm "base64 -w0 (attached) is caught" fire  'base64 -w0 < f'
# Flags bunched before the hazardous one must still be seen.
arm "a preceding flag does not hide it" fire 'grep -n -P "x" f'

# NOT A USE - this repo is full of prose about exactly these flags.
arm "a comment about sed -i is fine"  clean '# Not sed -i: GNU takes the suffix attached, BSD as the next arg'
arm "an indented comment is fine"     clean '    # stat -c is GNU and BSD rejects it'
# THE MARKER, on the line and on the line above.
arm "hazard-ok on the line allows it"  clean 'x=$(stat -c %Y f)   # hazard-ok: probe chose this branch'
arm "hazard-ok above allows it"        clean '# hazard-ok: platform probe
if stat -c %Y . >/dev/null 2>&1; then :; fi'
arm "hazard-ok-file exempts the file"  clean '# hazard-ok-file: this script is about the divergence
sed -i "s/a/b/" f'
# THE MARKER MUST BE A DECLARATION, NOT A MENTION. It used to be a substring match, so this gate
# never scanned its OWN source - its header documents the marker - and any file whose prose merely
# named the marker got a free pass on every hazard in it.
arm "prose naming the marker does NOT exempt" fire 'echo "use hazard-ok-file: to exempt a file"
sed -i "s/a/b/" f'
arm "a marker with no reason does NOT exempt" fire '# hazard-ok-file:
sed -i "s/a/b/" f'
# A HEREDOC BODY IS DATA. This gate keeps its own hazard table in one, so without this it would flag
# its own regexes the moment the marker was tightened.
arm "a hazard inside a heredoc is data"  clean 'cat <<EOF
sed -i "s/a/b/" f
EOF'
arm "a hazard AFTER a heredoc still fires" fire 'cat <<EOF
harmless prose
EOF
sed -i "s/a/b/" f'
# Similar-looking words must not trip it.
arm "a word ending in sed is fine"     clean 'parsed -i something'
arm "no hazards at all is clean"       clean 'echo hello'

# CANNOT RUN is not a pass - an empty scan directory must exit 2, not 0.
empty="$scratch/empty"; mkdir -p "$empty"
out="$(bash "$GATE" "$empty" 2>&1)"; rc=$?
if [ "$rc" -eq 2 ]; then printf 'ok:   an empty corpus is CANNOT RUN, not a pass\n'; pass=$((pass + 1))
else printf 'FAIL: empty corpus returned %s, expected 2\n' "$rc"; fail=$((fail + 1)); fi

printf '\n%s passed, %s failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
