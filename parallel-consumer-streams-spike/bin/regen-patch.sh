#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Re-derives src/main/patch/pcspike.patch from your edits to the generated sources.
#
# The workflow this exists to make bearable:
#
#   1. ./mvnw -pl parallel-consumer-streams-spike generate-sources
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
# The patch is the deliverable: its line count is the spike's answer to "how little had to change".

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
module_dir="$(dirname "$here")"

pristine="$module_dir/target/kafka-pristine"
patched="$module_dir/target/kafka-patched"
patch_file="$module_dir/src/main/patch/pcspike.patch"

for d in "$pristine" "$patched"; do
    if [[ ! -d "$d" ]]; then
        echo "regen-patch: missing $d" >&2
        echo "regen-patch: run './mvnw -pl parallel-consumer-streams-spike generate-sources' first" >&2
        exit 1
    fi
done

# diff exits 1 when files differ, which is the normal case here - so don't let set -e kill us.
set +e
(cd "$module_dir/target" && diff -ruN "kafka-pristine" "kafka-patched") >"$patch_file.tmp"
diff_status=$?
set -e

if [[ $diff_status -gt 1 ]]; then
    echo "regen-patch: diff failed with status $diff_status" >&2
    rm -f "$patch_file.tmp"
    exit 1
fi

# diff -ruN prefixes paths with the two directory names; strip them so the patch applies with -p1 from
# inside the generated directory, which is what apply-patch.sh does.
sed -e 's|^--- kafka-pristine/|--- a/|' \
    -e 's|^+++ kafka-patched/|+++ b/|' \
    "$patch_file.tmp" >"$patch_file"
rm -f "$patch_file.tmp"

hunks=$(grep -c '^@@' "$patch_file" || true)
files=$(grep -c '^--- ' "$patch_file" || true)
lines=$(wc -l <"$patch_file" | tr -d ' ')

if [[ "$hunks" -eq 0 ]]; then
    echo "regen-patch: no differences - patch is empty (control arm)"
else
    echo "regen-patch: wrote $patch_file"
    echo "regen-patch: $files file(s), $hunks hunk(s), $lines lines"
    echo "regen-patch: that line count is the spike's change-set size - quote it in the result doc"
fi
