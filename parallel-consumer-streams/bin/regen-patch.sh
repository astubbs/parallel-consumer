#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Re-derives src/main/patch/pc-streams.patch from your edits to the generated sources.
#
# The workflow this exists to make bearable:
#
#   1. ./mvnw -pl parallel-consumer-streams generate-sources
#        -> unpacks pristine Kafka sources into target/kafka-pristine
#        -> unpacks + patches a working copy into target/kafka-patched
#   2. edit the files under target/kafka-patched/ like normal Java
#   3. bin/regen-patch.sh
#        -> diffs pristine against your edits and rewrites the tracked patch
#   4. commit the patch. The generated trees are gitignored and never committed.
#
# Editing generated files feels wrong, and it is the honest cost of not committing Apache Kafka source
# into this repository (see the plan's KTD-S4). This script is what keeps that cost small.
#
# !! FOOT-GUN, READ THIS !!
# The unpack step runs with overWriteReleases=true, so ANY maven invocation between step 2 and step 3
# silently restores target/kafka-patched to (released sources + the *tracked* patch) and discards
# whatever you had just edited. It says nothing when it does this. It cost a full editing pass to learn.
# Rule of thumb: after you start editing, run regen-patch.sh BEFORE you run maven again. The hunk-count
# comparison at the end of this script is the tripwire - if it says the count went DOWN, you lost work.
#
# The patch is the deliverable: its line count is the spike's answer to "how little had to change".
#
# There is a second tree, generated at generate-test-sources in every build: Kafka's own test fixtures,
# which need the same accessor conversion for the same reason. Re-derive that one with:
#
#   bin/regen-patch.sh kafka-test-pristine kafka-test-patched src/main/patch/pc-streams-testfixtures.patch
#
# The three arguments are all-or-nothing; with none, the main tree above is assumed.

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
module_dir="$(dirname "$here")"

pristine_name="${1:-kafka-pristine}"
patched_name="${2:-kafka-patched}"
patch_rel="${3:-src/main/patch/pc-streams.patch}"

pristine="$module_dir/target/$pristine_name"
patched="$module_dir/target/$patched_name"
patch_file="$module_dir/$patch_rel"

for d in "$pristine" "$patched"; do
    if [[ ! -d "$d" ]]; then
        echo "regen-patch: missing $d" >&2
        echo "regen-patch: run './mvnw -pl parallel-consumer-streams generate-sources' first" >&2
        exit 1
    fi
done

# diff exits 1 when files differ, which is the normal case here - so don't let set -e kill us.
set +e
(cd "$module_dir/target" && diff -ruN "$pristine_name" "$patched_name") >"$patch_file.tmp"
diff_status=$?
set -e

if [[ $diff_status -gt 1 ]]; then
    echo "regen-patch: diff failed with status $diff_status" >&2
    rm -f "$patch_file.tmp"
    exit 1
fi

# Count what is already tracked BEFORE overwriting, so the foot-gun above is detectable rather than
# silent: if a stray maven run reverted the working tree, the regenerated patch is smaller than the one
# on disk and we are about to commit a loss.
hunks_before=$(grep -c '^@@' "$patch_file" 2>/dev/null || true)

# diff -ruN prefixes paths with the two directory names; strip them so the patch applies with -p1 from
# inside the generated directory, which is what apply-patch.sh does.
sed -e "s|^--- $pristine_name/|--- a/|" \
    -e "s|^+++ $patched_name/|+++ b/|" \
    "$patch_file.tmp" >"$patch_file"
rm -f "$patch_file.tmp"

hunks=$(grep -c '^@@' "$patch_file" || true)

if [[ "${hunks_before:-0}" -gt "$hunks" ]]; then
    echo "regen-patch: WARNING - hunk count went DOWN, $hunks_before -> $hunks." >&2
    echo "regen-patch: you have probably lost edits. The usual cause is a maven run between editing" >&2
    echo "regen-patch: target/kafka-patched/ and running this script - unpack overwrites without asking." >&2
    echo "regen-patch: the previous patch is recoverable with 'git checkout -- $patch_file'." >&2
fi
files=$(grep -c '^--- ' "$patch_file" || true)
lines=$(wc -l <"$patch_file" | tr -d ' ')

if [[ "$hunks" -eq 0 ]]; then
    echo "regen-patch: no differences - patch is empty (control arm)"
else
    echo "regen-patch: wrote $patch_file"
    echo "regen-patch: $files file(s), $hunks hunk(s), $lines lines"
    echo "regen-patch: that line count is the spike's change-set size - quote it in the result doc"
fi
