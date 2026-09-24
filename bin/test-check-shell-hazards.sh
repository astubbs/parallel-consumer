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

# SHARED GIT STATE - portable on every platform, and still a silent wrong answer everywhere else in
# the clone. Two of these shipped in bin/check-quarantine-owners.sh and re-shallowed the whole
# repository on every sweep of bin/check-all.sh.
arm "git fetch --depth= is caught"    fire  'git fetch --quiet --depth=1 origin master'
arm "git fetch --depth 1 is caught"   fire  'git fetch --depth 1 origin master'
arm "--shallow-since is caught"       fire  'git fetch --shallow-since=2026-01-01 origin master'
# `pull` is `fetch` plus a merge, and writes the same file. Matching only `fetch` would have left the
# obvious next spelling uncovered.
arm "git pull --depth is caught"      fire  'git pull --depth=1 origin master'
# A GLOBAL OPTION BEFORE THE SUBCOMMAND is the exact walk-past that let `git -C DIR rev-list` through
# .claude/hooks/check-shallow-history.sh, and `git --git-dir=... fetch` is the spelling the FIX uses.
arm "git --git-dir=X fetch --depth caught" fire 'git --git-dir=/tmp/x fetch --depth=1 https://h/r ref'
# A global option whose VALUE does not start with `-`: the element has to step over BOTH tokens, or
# the pattern is looking for `fetch` and finding `core.pager=cat`. This arm was written because the
# first version of the row missed exactly this - the hook's own header documents the same walk-past.
arm "git -c k=v fetch --depth caught" fire 'git -c core.pager=cat fetch --depth=1 origin master'
arm "git -C dir fetch --depth caught" fire 'git -C /tmp/x fetch --depth=1 origin master'
# NOT THE HAZARD. A clone creates its own repository, so its depth is nobody else's; and an
# unrestricted fetch never writes the `shallow` file.
arm "git clone --depth=1 is fine"     clean 'git clone -q --depth=1 "file://$o" "$d"'
arm "an undepthed git fetch is fine"  clean 'git fetch --no-tags origin master'
arm "hazard-ok allows a scratch fetch" clean '# hazard-ok: fetches into a throwaway git dir
git --git-dir="$scratch" fetch --depth=1 "$url" "$ref"'

# BUILTIN OPTION LOOKALIKE - nothing about the machine varies; an argument is read as a flag.
# The positive arms are the two spellings of the line that shipped: a skip banner whose format
# string starts with a hyphen, which bash's builtin printf rejects with exit 2. Only a CI runner
# reached it, so every local run was green while every CI skip failed the row it was skipping.
arm "printf single-quoted dash format"   fire  'printf '\''- %s\n'\'' "$v"'
arm "printf double-quoted dash format"   fire  'printf "-x %s" "$v"'
# NOT THE HAZARD, and each is a spelling somebody will reach for while fixing the above.
arm "printf -- disarms it"               clean 'printf -- '\''- %s\n'\'' "$v"'
arm "printf -v is a real option"         clean 'printf -v out "%s" "$v"'
arm "a dash inside the format is fine"   clean 'printf "%s - %s" "$a" "$b"'
arm "a dash-led string that is not a format" clean 'echo "$m" | grep -F -- "- %s"'

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
# LINE CONTINUATIONS. The scanner reads LOGICAL commands, not physical lines, because no regex can
# see across a newline: `git fetch \` on one line and `--depth=1` on the next is one command to the
# shell and two unrelated lines to grep, so the physical-line scanner reported SUCCESS over exactly
# the fetch this category exists to stop. Every other row has the same hole, hence a gnu-bsd arm too.
arm "a continued git fetch --depth is caught" fire 'git fetch \
    --depth=1 origin master'
arm "a continued sed -i is caught"     fire 'sed \
    -i "s/a/b/" f'
# THE MARKER TRAVELS WITH THE COMMAND, so it counts on ANY physical line of it - the natural place to
# write it is beside the flag being excused, which is rarely the first line.
arm "hazard-ok on a later line of a continued command" clean 'git fetch \
    --depth=1 "$url" "$ref"   # hazard-ok: fetches into a throwaway git dir'
arm "hazard-ok above a continued command" clean '# hazard-ok: fetches into a throwaway git dir
git fetch \
    --depth=1 "$url" "$ref"'
# A HEREDOC BODY IS DATA EVEN WHEN IT LOOKS LIKE A CONTINUATION - joining inside one splices two
# lines of fixture text into a command the file never runs, which is the false-positive class that
# gets a gate switched off. The NESTED opener is why this arm is a control and not a restatement of
# "a hazard inside a heredoc is data": the range check further down is confused by an opener inside a
# body (it re-arms on `cat <<EOF` and then pairs the WRONG terminator), so lines 3-4 here are outside
# every range it computes. The joiner refusing to join them is the only thing left. Verified red
# against a heredoc-blind joiner, which reports `sed -i` at line 3 of a file that never runs sed.
arm "a continuation inside a heredoc is data" clean 'cat <<OUTER
sed \
    -i "s/a/b/" f
cat <<EOF
EOF
OUTER'
# A WRAPPER FUNCTION IS STILL A USE. `git` is routinely wrapped to redirect it - GIT_DIR, -C,
# credentials - and this PR's own commit 4 did exactly that: `git --git-dir=X fetch --quiet --depth=1`
# became `preview_git fetch --quiet --depth=1` in bin/check-quarantine-owners.sh, and gate coverage of
# that file silently dropped to zero while its `hazard-ok:` marker went on looking load-bearing. A
# check that quietly stops checking is the worst failure available to one, so the command word may
# now carry a `<name>_` or `<name>-` prefix on EVERY row.
arm "a _git wrapper fetch is caught"   fire  'preview_git fetch --quiet --depth=1 --no-tags "$u" "$r"'
arm "a _sed wrapper is caught"         fire  'my_sed -i "s/a/b/" f'
# THE SEPARATOR IS WHAT KEEPS THE WIDENING HONEST. Requiring the prefix to end in `_` or `-` is what
# stops an ordinary word that merely ENDS in the tool name from reading as a call to it - `digit` here
# and `parsed` below are the two halves of the same control. It also keeps `-` out of the front of a
# command word, so the `diff --git a/x b/x` in bin/test-check-pr-analysis-surfaces.sh cannot read as
# a wrapper named `--git`.
arm "a word merely ending in git is fine" clean 'digit fetch --depth=1 origin master'
# Similar-looking words must not trip it.
arm "a word ending in sed is fine"     clean 'parsed -i something'
arm "no hazards at all is clean"       clean 'echo hello'

# A FINDING ON A CONTINUED COMMAND POINTS AT THE LINE THE COMMAND STARTS ON, not the line the flag
# happens to sit on. That is where the reader has to edit, and the flag's own line is meaningless
# without the subcommand above it. The fixture's shebang is line 1, so the command starts at line 2.
d="$scratch/continued_lineno"; mkdir -p "$d"
printf '#!/usr/bin/env bash\ngit fetch \\\n    --depth=1 origin master\n' > "$d/subject.sh"
out="$(bash "$GATE" "$d" 2>&1)"; rc=$?
case "$out" in
    *"subject.sh:2 "*) printf 'ok:   a continued finding reports its first line\n'; pass=$((pass + 1)) ;;
    *) printf 'FAIL: continued finding did not report the start line (rc=%s)\n%s\n' "$rc" "$out"; fail=$((fail + 1)) ;;
esac

# A ROW'S REGEX IS MATCHED AGAINST THE COMMAND, NOT AGAINST A LINE-NUMBERED RECORD. This is a
# ROW-LEVEL contract, and `arm` cannot express it: it drives the whole gate, whose table is fixed. So
# the probe appends one row of its own to a COPY of the gate and runs that - the only way to assert
# what an author of a future row is entitled to assume. `^` is the natural way to write "at the start
# of a command", and the whole premise of this file is that people will add rows; a haystack carrying
# a `NNN:` prefix would make every such row match nothing, for ever, silently. That is this gate's own
# subject matter turned inward, so it is tested rather than commented.
mkdir -p "$scratch/lib"; cp "$(dirname "$GATE")/lib/shell-corpus.sh" "$scratch/lib/"
probe="$scratch/anchor-probe.sh"
awk '/^HZ$/ && !seen { print "gnu-bsd\t^anchored_probe\tprobe row: a ^-anchored pattern must see the"; seen = 1 }
     { print }' "$GATE" > "$probe"
d="$scratch/anchor_probe"; mkdir -p "$d"
printf '#!/usr/bin/env bash\nanchored_probe --now\n' > "$d/subject.sh"
out="$(bash "$probe" "$d" 2>&1)"; rc=$?
if [ "$rc" -ne 0 ]; then printf 'ok:   a ^-anchored row matches a command at the start of a line\n'; pass=$((pass + 1))
else printf 'FAIL: a ^-anchored row matched nothing - the haystack is not the command (rc=%s)\n%s\n' \
    "$rc" "$out"; fail=$((fail + 1)); fi

# CANNOT RUN is not a pass - an empty scan directory must exit 2, not 0.
empty="$scratch/empty"; mkdir -p "$empty"
out="$(bash "$GATE" "$empty" 2>&1)"; rc=$?
if [ "$rc" -eq 2 ]; then printf 'ok:   an empty corpus is CANNOT RUN, not a pass\n'; pass=$((pass + 1))
else printf 'FAIL: empty corpus returned %s, expected 2\n' "$rc"; fail=$((fail + 1)); fi

printf '\n%s passed, %s failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
