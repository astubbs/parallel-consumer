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
# THE THIRD CATEGORY IS BUILTIN-OPTION-LOOKALIKE, and it is the same signature reached from a
# different direction: nothing about the machine varies, but an ARGUMENT is read as a flag. A quoted
# `printf` format beginning with a hyphen is the instance that arrived - the builtin rejects it,
# returns 2, and `set -e` publishes that as the script's verdict. It stayed invisible for exactly the
# reason this table exists: the only caller that reached it ran on CI, where $GITHUB_STEP_SUMMARY is
# set, so every local run was green. ShellCheck does not model builtin option parsing and passed it.
#
# ITS REGEX SPELLS THE OPENING QUOTE AS punctuation-that-is-not-a-dash, NOT AS A QUOTE CLASS, and
# that is forced rather than clever: this table is a quoted heredoc inside a command substitution,
# and bash 3.2 - the bash macOS ships, which this repo supports - mis-parses an UNBALANCED quote
# character in one. A row containing a quote class fails to parse the whole script. Verified against
# 3.2.57 directly; every other row happens to balance its quotes and so has never met this.
#
# THE SECOND CATEGORY IS SHARED-GIT-STATE, and it is what the generic shape was for. `git fetch
# --depth` is portable and correct on every platform; what it does silently is write the `shallow`
# file in the shared --git-common-dir, truncating history for every worktree of the clone. Same
# signature - no error, wrong answers elsewhere, unreachable by a linter - so it is a table row.
#
# ONE PIECE OF CONTROL FLOW EARNS ITS PLACE, and it is deliberately not a table row: before anything
# is matched, backslash continuations are joined so every regex above sees LOGICAL commands rather
# than physical lines. No regex can be written that sees across a newline, which is why this cannot
# be data - and without it the table is quietly optional. `git fetch \` on one line with
# `--depth=1 origin master` on the next is the ordinary way to format that command, and the
# physical-line scanner reported SUCCESS over it: the subcommand and the flag never appeared in the
# same haystack. Every other row has the same hole (`sed \` then `-i ...`), so the join strengthens
# all of them at once. A finding on a joined command reports the line the command STARTS on, because
# that is where a reader has to edit and the flag's own line means nothing without it.
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
# ZERO FINDINGS TODAY, ON PURPOSE - for the gnu-bsd rows. Every existing use in this repo is already
# correct: `stat` sits behind a probe-then-choose, `sed -i` and `date -d` were deliberately avoided.
# Those rows fix nothing; they stop the next script from re-learning it, which makes them free to
# adopt. The shared-git-state row is the exception and had two live findings when it landed, both in
# bin/check-quarantine-owners.sh, fixed in the same change.
#
# THE MARKER, not an allowlist in this file. A use that is genuinely correct carries
# `hazard-ok: <reason>` on the line or the line above, so the reason travels with the code instead
# of rotting in a list here - and on a continued command, ANY of its physical lines counts, because
# the natural place to write the excuse is beside the flag being excused rather than up on the first
# line. `hazard-ok-file: <reason>` anywhere in a file exempts all of it.
set -uo pipefail

# shellcheck source=lib/shell-corpus.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/shell-corpus.sh" || exit 1
shell_corpus_init "${1:-}" || exit 1

# <category><TAB><extended-regex><TAB><what actually happens on the other platform>
#
# THE COMMAND WORD MAY BE A WRAPPER, hence the `([[:alnum:]_]*[_-])?` after every row's leading
# boundary. `git` is routinely wrapped to redirect it, and a refactor here proved the point: the
# quarantine gate's `git --git-dir=X fetch --quiet --depth=1` became `preview_git fetch --quiet
# --depth=1` behind a one-line wrapper, the row stopped matching, and that file's coverage silently
# went to zero - while the `hazard-ok:` marker above the call went on looking load-bearing. Applied
# to EVERY row rather than just the git one: a `my_sed` wrapper is less idiomatic than a `preview_git`
# one, but a row whose command-word class differs from its neighbours is its own trap, and the header
# above already argues that a row finding nothing today earns its place by stopping the next script.
# THE `[_-]` IS LOAD-BEARING - it demands a separator, so `digit` and `parsed` are not calls to `git`
# and `sed`, and a command word can never begin with `-` (`diff --git a/x` is not a wrapper).
#
# A FLAG THAT TAKES AN ATTACHED VALUE NEEDS A LOOSER TAIL. `-i([[:space:]]|$)` misses `sed -i.bak`
# and `-w([[:space:]]|$)` misses `base64 -w0` - and in both cases the attached spelling is the
# GNU-only one, so the strict pattern let exactly the dangerous form through while catching the
# benign one. Caught by the self-test, which is why it carries an attached-value arm.
HAZARDS=$(cat <<'HZ'
gnu-bsd	(^|[^[:alnum:]_-])([[:alnum:]_]*[_-])?sed[[:space:]]+(-[[:alnum:]]+[[:space:]]+)*-i[^[:space:]]*([[:space:]]|$)	GNU takes the backup suffix ATTACHED (-i.bak); BSD takes it as the NEXT ARGUMENT, so `sed -i` silently consumes whatever follows. Write a temp file and move it.
gnu-bsd	(^|[^[:alnum:]_-])([[:alnum:]_]*[_-])?stat[[:space:]]+(-[[:alnum:]]+[[:space:]]+)*-c([[:space:]]|$)	`stat -c` is GNU; BSD/macOS rejects it. Probe with `if stat -c %Y . >/dev/null 2>&1` and fall back to `stat -f %m`.
gnu-bsd	(^|[^[:alnum:]_-])([[:alnum:]_]*[_-])?stat[[:space:]]+(-[[:alnum:]]+[[:space:]]+)*-f([[:space:]]|$)	`stat -f` is BSD; on GNU it exits 1 while PRINTING filesystem prose to stdout, so a caller capturing it gets prose, not a number.
gnu-bsd	(^|[^[:alnum:]_-])([[:alnum:]_]*[_-])?date[[:space:]]+(-[[:alnum:]]+[[:space:]]+)*-d([[:space:]]|$)	`date -d` is GNU; BSD date reads -d as a daylight-saving flag. Use python3 for date arithmetic.
gnu-bsd	(^|[^[:alnum:]_-])([[:alnum:]_]*[_-])?readlink[[:space:]]+(-[[:alnum:]]+[[:space:]]+)*-f([[:space:]]|$)	`readlink -f` is GNU and absent from older macOS. Use `cd ... && pwd -P`, or python3 realpath.
gnu-bsd	(^|[^[:alnum:]_-])([[:alnum:]_]*[_-])?grep[[:space:]]+(-[[:alnum:]]+[[:space:]]+)*-P([[:space:]]|$)	`grep -P` needs PCRE and is absent from BSD grep. Use -E.
gnu-bsd	(^|[^[:alnum:]_-])([[:alnum:]_]*[_-])?sort[[:space:]]+(-[[:alnum:]]+[[:space:]]+)*-V([[:space:]]|$)	`sort -V` is GNU; BSD sort has no version sort and treats it as an error.
gnu-bsd	(^|[^[:alnum:]_-])([[:alnum:]_]*[_-])?base64[[:space:]]+(-[[:alnum:]]+[[:space:]]+)*-w[0-9]*([[:space:]]|$)	`base64 -w` is GNU; BSD base64 does not wrap and rejects -w.
gnu-bsd	(^|[^[:alnum:]_-])([[:alnum:]_]*[_-])?sed[[:space:]]+(-[[:alnum:]]+[[:space:]]+)*-r([[:space:]]|$)	`sed -r` is GNU; -E works on both and means the same thing.
builtin-option-lookalike	(^|[^[:alnum:]_-])printf[[:space:]]+[^-[:space:][:alnum:]]-	A quoted `printf` format that STARTS with a hyphen is read as an OPTION by bash's builtin printf: it fails with "invalid option", returns 2, and under `set -e` that becomes the script's exit code. Write `printf -- '-...'`. Observed live: a skip banner appended to $GITHUB_STEP_SUMMARY turned every CI skip into a failed row while every developer box stayed green, because the variable is only set on a runner.
shared-git-state	(^|[^[:alnum:]_-])([[:alnum:]_]*[_-])?git[[:space:]]+((-[^[:space:]]+)([[:space:]]+[^-[:space:]][^[:space:]]*)?[[:space:]]+)*(fetch|pull)([[:space:]]+[^[:space:]]+)*[[:space:]]+--(depth|shallow-since|shallow-exclude)	`git fetch --depth` (and `git pull --depth`) writes the `shallow` file, which lives in the SHARED --git-common-dir - so it truncates history for EVERY worktree of the clone at once, and merge-base and ahead/behind then answer confidently wrong instead of erroring. Fetch into a throwaway git dir and read FETCH_HEAD there; mark it hazard-ok when the fetch target really is disposable.
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
    # LOGICAL COMMANDS, ONE PER RECORD, as `<first-physical-line>:<joined text>` - see the header for
    # why this one transformation is control flow rather than a table row. Three things it must get
    # right, each of which has its own self-test arm:
    #   - a COMMENT ends at the newline, so a `\` inside one continues nothing. bash agrees: `# x \`
    #     followed by `echo hi` runs the echo.
    #   - a HEREDOC BODY is data, so its lines are emitted one per record and never joined - splicing
    #     two lines of fixture text into a command the file never runs is the false-positive class
    #     that gets a gate switched off. Emitting them with their true line numbers is also what
    #     keeps the heredoc-range check below working unchanged.
    #   - the joined text keeps every physical line, which is what lets a `hazard-ok:` marker written
    #     beside the offending flag suppress the whole command.
    # The heredoc opener is recognised exactly as the range scan further down recognises it, so the
    # two cannot disagree about where a body starts.
    norm=$(awk '
        delim != "" {
            print NR ":" $0
            if ($0 ~ "^[[:space:]]*" delim "[[:space:]]*$") delim = ""
            next
        }
        {
            line = $0
            if (pending == 0) { start = NR; buf = "" }
            if (line ~ /\\$/ && !(pending == 0 && line ~ /^[[:space:]]*#/)) {
                sub(/\\$/, "", line); buf = buf line; pending = 1; next
            }
            buf = buf line
            print start ":" buf
            pending = 0
            if ($0 ~ /<<-?[\x27"]?[A-Za-z_][A-Za-z_0-9]*[\x27"]?$/) {
                d = $0; sub(/.*<<-?/, "", d); gsub(/[\x27"]/, "", d); delim = d
            }
        }
        END { if (pending) print start ":" buf }' "$f")
    # MATCHED AGAINST THE COMMAND TEXT ALONE - the line number travels alongside, never inside the
    # haystack. A row is DATA that someone will add, and `^` is the natural way to write "at the
    # start of a command"; a haystack still carrying its `NNN:` prefix would make every such row
    # match nothing, for ever, with the gate reporting success - this file's own subject matter,
    # turned inward. The stripped copy is derived FROM $norm rather than produced by a second awk
    # run, so the two cannot drift into disagreeing about how many records there are, which is what
    # the index mapping below depends on.
    norm_text=$(sed 's/^[0-9][0-9]*://' <<<"$norm")
    while IFS= read -r hz; do
        [ -n "$hz" ] || continue
        cat_name="${hz%%	*}"; rest="${hz#*	}"
        pat="${rest%%	*}"; why="${rest#*	}"
        # FED FROM THE STRIPPED RECORDS (a herestring, not a pipe - `printf | grep` under pipefail
        # inverts its own answer, the rule bin/check-shell-sigpipe.sh enforces). `idx` is grep's
        # RECORD index, which is the same index in $norm because norm_text is $norm line for line, so
        # the prefixed copy hands back the physical line the command starts on. `text` is the whole
        # joined command, which is what the marker and comment tests want, while the heredoc-range
        # test still gets a real physical line because heredoc bodies are emitted one per record.
        while IFS=: read -r idx text; do
            [ -n "$idx" ] || continue
            rec=$(sed -n "${idx}p" <<<"$norm"); lineno="${rec%%:*}"
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
$(grep -nE "$pat" <<<"$norm_text" 2>/dev/null || true)
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
