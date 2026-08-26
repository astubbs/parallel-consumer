#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# The shell corpus the bespoke shell gates scan: `bin/*.sh` plus `.claude/hooks/*.sh`.
#
# WHY BOTH DIRECTORIES. The hooks are shell that runs on every agent session with no interactive user
# to notice breakage - exactly the population these guards exist for - and check-shell-sigpipe.sh was
# blind to them until review found a live instance there: the session index piped into `grep -q`
# under pipefail, harmless only because its input was well under the 64KiB pipe buffer that triggers
# the inversion. A gate that scans half the corpus is a gate that reports clean about the half it
# looked at.
#
# WHY THIS IS A LIB. check-shell-sigpipe.sh and check-shell-hazards.sh had this resolution
# character-for-character duplicated, and it had ALREADY DRIFTED: sigpipe changed directory only when
# given no argument, while hazards did it unconditionally, so an explicit RELATIVE scan directory
# resolved against different roots in the two gates. Nothing would have caught that - both self-tests
# pass absolute paths. Sigpipe's behaviour is the correct one and is what this lib does: an explicit
# path is the caller's, so do not move out from under it.
#
# WHY SETTING VARIABLES RATHER THAN ECHOING A LIST. `dirs=$(resolve)` runs the function in a SUBSHELL,
# so a `cd` inside it would be discarded and the caller would scan relative to wherever it started -
# silently, and only in the no-argument case that CI actually uses. Assigning to a variable keeps the
# directory change in the caller's shell, where it has to happen.

# Sets SHELL_CORPUS_DIRS, and moves to the repo root when scanning the default corpus.
shell_corpus_init() { # [<explicit-scan-dir>]
    if [ -z "${1:-}" ]; then
        cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)" || return 1
        # THE `lib/` DIRECTORIES COUNT TOO, and this file is the proof of why they are easy to miss:
        # `shell_corpus_files` globs `"$d"/*.sh`, which does not recurse, so `bin/lib/` and
        # `.claude/hooks/lib/` were scanned by neither gate - including this very file. A shared
        # helper is the worst place for a silent-failure bug, because every caller inherits it.
        # Named explicitly rather than made recursive: the corpus is a list of directories whose
        # contents are shell, and a `find` would start reading fixtures.
        SHELL_CORPUS_DIRS="bin bin/lib .claude/hooks .claude/hooks/lib"
    else
        SHELL_CORPUS_DIRS="$1"
    fi
}

# Every script in the corpus, one per line. Empty output is a legitimate answer - each caller decides
# whether that is "nothing in scope" or "cannot run", and they do not agree, so this does not decide.
shell_corpus_files() {
    for d in $SHELL_CORPUS_DIRS; do ls "$d"/*.sh 2>/dev/null; done
}
