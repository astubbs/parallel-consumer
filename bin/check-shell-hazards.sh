#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Bespoke scanner for shell hazards that ANSWER WRONGLY rather than erroring - the class ShellCheck
# structurally cannot see.
#
# WHY A SECOND SHELL CHECKER. ShellCheck analyses the shell LANGUAGE: quoting, unused variables,
# bashisms under `-s sh`. Everything here is about what a command MEANS on the machine it lands on -
# which flags an implementation accepts, what it prints when it disagrees. A linter that does not
# model coreutils cannot reach it, and every hazard below was found the hard way in this repo.
# (NOTE: the `NOTE:` prefix is load-bearing - a comment whose first word is `shellcheck` is parsed as
# a directive, the same trap check-shell-sigpipe.sh documents.)
#
# DELIBERATELY GENERIC, because this will grow. Hazards are DATA - a category, a regex, and the
# sentence explaining the divergence - so adding one is a line in the table below rather than new
# control flow. The first category is GNU-vs-BSD coreutils divergence; the shape suits anything with
# the same signature: silent, platform- or version-dependent, invisible to a linter.
#
# check-shell-sigpipe.sh BELONGS IN HERE, and has not moved yet. It is the same class by every test
# that matters: a silent wrong answer, invisible to ShellCheck, found the hard way. It predates this
# file, which is the only reason it is separate - and this file is named for the class rather than
# for today's members precisely so it can absorb it.
#
# WHAT ABSORBING IT NEEDS, so the next person does not rediscover it: the table would need a
# per-hazard PRECONDITION field, because piping into `grep -q` is only a defect when the file sets
# `pipefail`, and every entry here currently applies unconditionally. Its sixteen self-test arms move
# with it, `.githooks/pre-commit` names it in a hardcoded list, and roughly a dozen comments across
# bin/ and docs/ cite it as the authority for the herestring rule. Tracked in
# docs/inflight/ci-fold-sigpipe-into-shell-hazards.md.
#
# The corpus resolution both gates share already lives in bin/lib/shell-corpus.sh, so they cannot
# drift about WHAT they scan while they remain separate. They already had.
#
# ZERO FINDINGS TODAY, ON PURPOSE. Every existing use in this repo is already correct: `stat` sits
# behind a probe-then-choose, `sed -i` and `date -d` were deliberately avoided. This gate fixes
# nothing; it stops the next script from re-learning it, which makes it free to adopt.
#
# THE MARKER, not an allowlist in this file. A use that is genuinely correct carries
# `hazard-ok: <reason>` on the line or the line above, so the reason travels with the code instead
# of rotting in a list here. `hazard-ok-file: <reason>` anywhere in a file exempts all of it.
set -uo pipefail

# shellcheck source=lib/shell-corpus.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/shell-corpus.sh" || exit 1
shell_corpus_init "${1:-}" || exit 1

# <category><TAB><extended-regex><TAB><what actually happens on the other platform>
#
# A FLAG THAT TAKES AN ATTACHED VALUE NEEDS A LOOSER TAIL. `-i([[:space:]]|$)` misses `sed -i.bak`
# and `-w([[:space:]]|$)` misses `base64 -w0` - and in both cases the attached spelling is the
# GNU-only one, so the strict pattern let exactly the dangerous form through while catching the
# benign one. Caught by the self-test, which is why it carries an attached-value arm.
HAZARDS=$(cat <<'HZ'
gnu-bsd	(^|[^[:alnum:]_-])sed[[:space:]]+(-[[:alnum:]]+[[:space:]]+)*-i[^[:space:]]*([[:space:]]|$)	GNU takes the backup suffix ATTACHED (-i.bak); BSD takes it as the NEXT ARGUMENT, so `sed -i` silently consumes whatever follows. Write a temp file and move it.
gnu-bsd	(^|[^[:alnum:]_-])stat[[:space:]]+(-[[:alnum:]]+[[:space:]]+)*-c([[:space:]]|$)	`stat -c` is GNU; BSD/macOS rejects it. Probe with `if stat -c %Y . >/dev/null 2>&1` and fall back to `stat -f %m`.
gnu-bsd	(^|[^[:alnum:]_-])stat[[:space:]]+(-[[:alnum:]]+[[:space:]]+)*-f([[:space:]]|$)	`stat -f` is BSD; on GNU it exits 1 while PRINTING filesystem prose to stdout, so a caller capturing it gets prose, not a number.
gnu-bsd	(^|[^[:alnum:]_-])date[[:space:]]+(-[[:alnum:]]+[[:space:]]+)*-d([[:space:]]|$)	`date -d` is GNU; BSD date reads -d as a daylight-saving flag. Use python3 for date arithmetic.
gnu-bsd	(^|[^[:alnum:]_-])readlink[[:space:]]+(-[[:alnum:]]+[[:space:]]+)*-f([[:space:]]|$)	`readlink -f` is GNU and absent from older macOS. Use `cd ... && pwd -P`, or python3 realpath.
gnu-bsd	(^|[^[:alnum:]_-])grep[[:space:]]+(-[[:alnum:]]+[[:space:]]+)*-P([[:space:]]|$)	`grep -P` needs PCRE and is absent from BSD grep. Use -E.
gnu-bsd	(^|[^[:alnum:]_-])sort[[:space:]]+(-[[:alnum:]]+[[:space:]]+)*-V([[:space:]]|$)	`sort -V` is GNU; BSD sort has no version sort and treats it as an error.
gnu-bsd	(^|[^[:alnum:]_-])base64[[:space:]]+(-[[:alnum:]]+[[:space:]]+)*-w[0-9]*([[:space:]]|$)	`base64 -w` is GNU; BSD base64 does not wrap and rejects -w.
gnu-bsd	(^|[^[:alnum:]_-])sed[[:space:]]+(-[[:alnum:]]+[[:space:]]+)*-r([[:space:]]|$)	`sed -r` is GNU; -E works on both and means the same thing.
HZ
)

problems=0
scanned=0

for f in $(shell_corpus_files); do
    [ -f "$f" ] || continue
    # COUNTED BEFORE THE EXEMPTION, deliberately. `scanned` exists to tell "no scripts here, this
    # gate cannot run" from "scripts here, all clean" - a distinction this repo treats as load
    # bearing everywhere. Counting after the skip made a corpus of exempted files look EMPTY, so the
    # gate reported CANNOT RUN over files it had deliberately passed. Found by the self-test.
    scanned=$((scanned + 1))
    # A REAL DECLARATION, not a substring. This was `grep -q 'hazard-ok-file:'`, which matched the
    # header comment in THIS file documenting the marker - so the gate never scanned its own source,
    # and any file whose prose merely mentioned the marker got a free pass on every hazard in it.
    # Anchored to a comment line with a reason after it, so writing ABOUT the marker cannot disarm it.
    grep -qE '^[[:space:]]*#[[:space:]]*hazard-ok-file:[[:space:]]*[^[:space:]]' "$f" && continue
    while IFS= read -r hz; do
        [ -n "$hz" ] || continue
        cat_name="${hz%%	*}"; rest="${hz#*	}"
        pat="${rest%%	*}"; why="${rest#*	}"
        while IFS=: read -r lineno text; do
            [ -n "$lineno" ] || continue
            # A COMMENT ABOUT A HAZARD IS NOT A USE OF IT, and this repo is full of comments warning
            # about exactly these flags - they are how the knowledge survived before this gate. A
            # scanner that flagged its own documentation would be abandoned within a day.
            case "$(printf '%s' "$text" | sed 's/^[[:space:]]*//')" in '#'*) continue ;; esac
            # A HEREDOC BODY IS DATA, exactly like a comment. This file keeps its own hazard table in
            # one, so tightening the file marker above without this would turn the gate red on its own
            # regexes - and other scripts keep fixture text and generated documentation in them too.
            # Residual, stated rather than hidden: a heredoc that GENERATES a script would have its
            # hazards missed. Nothing here does that, and the alternative - flagging every fixture and
            # every worked example - is the false-positive class that gets a gate switched off.
            in_heredoc=0
            while IFS= read -r hd; do
                [ -n "$hd" ] || continue
                hd_start="${hd%%:*}"; hd_end="${hd##*:}"
                if [ "$lineno" -gt "$hd_start" ] && [ "$lineno" -lt "$hd_end" ]; then in_heredoc=1; break; fi
            done <<EOF
$(awk '/<<-?[\x27"]?[A-Za-z_][A-Za-z_0-9]*[\x27"]?$/ {
         d=$0; sub(/.*<<-?/,"",d); gsub(/[\x27"]/,"",d); start=NR; delim=d; next }
       delim != "" && $0 ~ "^[[:space:]]*"delim"[[:space:]]*$" { print start":"NR; delim="" }' "$f")
EOF
            [ "$in_heredoc" -eq 1 ] && continue
            case "$text" in *hazard-ok:*) continue ;; esac
            prev=$(sed -n "$((lineno - 1))p" "$f" 2>/dev/null)
            case "$prev" in *hazard-ok:*) continue ;; esac
            echo "HAZARD[$cat_name]: $f:$lineno  $why" >&2
            printf '    %s\n' "$(printf '%s' "$text" | sed 's/^[[:space:]]*//' | cut -c1-110)" >&2
            problems=$((problems + 1))
        done <<EOF
$(grep -nE "$pat" "$f" 2>/dev/null || true)
EOF
    done <<EOF
$HAZARDS
EOF
done

if [ "$scanned" -eq 0 ]; then
    echo "check-shell-hazards: no scripts found in $SHELL_CORPUS_DIRS - CANNOT RUN" >&2
    exit 2
fi

if [ "$problems" -gt 0 ]; then
    echo "check-shell-hazards: $problems hazard(s)." >&2
    echo "  These do not ERROR on the wrong platform - they answer differently. If a use is correct" >&2
    echo "  (a platform probe, or detection already chose the branch), mark it: hazard-ok: <why>" >&2
    exit 1
fi

echo "ok:   no silent-divergence hazards in $SHELL_CORPUS_DIRS"
