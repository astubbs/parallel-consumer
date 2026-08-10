#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Applies the spike's patch to the Kafka sources unpacked into the generated-sources tree.
#
# Called from the pom at generate-sources. Two properties matter:
#   $1  the directory holding the freshly unpacked (pristine) Kafka sources
#   $2  the patch file to apply
#
# An EMPTY patch file is an explicit, successful no-op - that is U3's control arm, where the generated
# classes must be byte-for-byte the released sources so any behaviour difference is the harness's fault
# and nothing else's. We check emptiness ourselves rather than relying on `patch`, whose behaviour on
# empty input differs between the BSD patch on macOS and GNU patch on CI.
#
# We dry-run before applying because a half-applied patch is the worst failure available here: it
# produces a tree that compiles but is not the thing anyone reviewed.

set -euo pipefail

target_dir="${1:?usage: apply-patch.sh <target-dir> <patch-file>}"
patch_file="${2:?usage: apply-patch.sh <target-dir> <patch-file>}"

if [[ ! -d "$target_dir" ]]; then
    echo "apply-patch: target directory does not exist: $target_dir" >&2
    exit 1
fi

if [[ ! -f "$patch_file" ]]; then
    echo "apply-patch: no patch file at $patch_file - nothing to apply"
    exit 0
fi

# Strip comments and blank lines to decide emptiness, so a patch file that only carries an explanatory
# header still counts as "no change".
if ! grep -qE '^(---|\+\+\+|@@|diff )' "$patch_file"; then
    echo "apply-patch: $patch_file contains no hunks - control arm, generated sources left as released"
    exit 0
fi

cd "$target_dir"

if ! patch -p1 --dry-run --forward <"$patch_file" >/dev/null 2>&1; then
    echo "apply-patch: FAILED - $patch_file does not apply cleanly to the unpacked sources." >&2
    echo "apply-patch: this usually means kafka.version moved and the patch needs re-deriving." >&2
    echo "apply-patch: re-run with the diagnostics below, then use bin/regen-patch.sh." >&2
    patch -p1 --dry-run --forward <"$patch_file" >&2 || true
    exit 1
fi

patch -p1 --forward <"$patch_file"
echo "apply-patch: applied $(grep -c '^@@' "$patch_file") hunk(s) from $patch_file"
