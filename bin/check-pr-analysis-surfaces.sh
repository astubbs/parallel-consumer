#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# EVERY ANALYSIS FINDING THAT LANDS ON A FILE YOUR PR TOUCHES, from every surface, in one command.
#
# WHY THIS EXISTS. The tools in this repo report to five different places, and no two of them are the
# same place: the Maven console, GitHub check annotations on the Files Changed tab, sticky PR
# comments, job summaries, and artifact files inside `target/`. Checking a PR against all five is a
# scavenger hunt, so in practice nobody does it - and the failure is silent, because a finding nobody
# read looks exactly like a finding that does not exist.
#
# It is not a hypothetical. astubbs/parallel-consumer#356 is the PR that TURNED ON `-Xlint:all` and
# SpotBugs-over-test-code. Both immediately fired on files that same PR was editing - a deprecated
# call and an unchecked conversion in `AbstractParallelEoSStreamProcessor`, and an fb-contrib finding
# on the exact `PCMetricsDef.toCamelCase` line the PR had just rewritten to satisfy a DIFFERENT
# detector. The author never looked at any of them. They were found by a human scrolling the Files
# Changed tab. An agent that switches a channel on and does not read it has not enabled analysis; it
# has enabled a channel.
#
# SCOPE: gh READS only, which is what keeps the `check-` prefix honest - see bin/AGENTS.md, "Naming a
# script here can grant it to the PR reviewer". It writes nothing, posts nothing, and needs no token
# beyond whatever `gh` already has.
#
# WHAT IT CANNOT SEE, stated so a clean run is not mistaken for a clean PR:
#   - Job summaries (PIT's survivor table, the CVE tables) are not exposed by the REST API at all.
#     The run URLs are printed instead; those you open by hand.
#   - Console-only output. A finding a tool prints to the Maven log and nowhere else is invisible
#     here, which is the argument for making tools annotate rather than merely print.
#   - Findings on files your PR does NOT touch. Deliberate: this answers "did I make it worse", not
#     "is the codebase clean". The standing haul is the registries' job.
#
# Exit codes: 0 nothing on your files, 1 findings on files you changed, 2 CANNOT RUN.
# The 2 matters - "gh is not authenticated" must never read as "no findings", the same fail-closed
# contract bin/ci-mutation-test.sh and bin/infer-test.sh hold.

set -euo pipefail

REPO="${PR_SURFACES_REPO:-astubbs/parallel-consumer}"
PR="${1:-}"

if ! command -v gh > /dev/null 2>&1; then
    echo "check-pr-analysis-surfaces: gh is not installed - CANNOT RUN (this is not a pass)." >&2
    exit 2
fi
if ! command -v python3 > /dev/null 2>&1; then
    echo "check-pr-analysis-surfaces: python3 is not installed - CANNOT RUN." >&2
    exit 2
fi

if [ -z "$PR" ]; then
    PR="$(gh pr view --repo "$REPO" --json number -q .number 2>/dev/null || true)"
fi
if [ -z "$PR" ]; then
    echo "check-pr-analysis-surfaces: no PR given and none for the current branch - CANNOT RUN." >&2
    echo "  Usage: bin/check-pr-analysis-surfaces.sh [PR_NUMBER]" >&2
    exit 2
fi

echo "check-pr-analysis-surfaces: PR #${PR} in ${REPO}"

# The head SHA is what check runs hang off. A PR number alone is not enough: annotations are per
# commit, so asking about the PR without pinning the head silently mixes in a superseded push.
HEAD_SHA="$(gh pr view "$PR" --repo "$REPO" --json headRefOid -q .headRefOid 2>/dev/null || true)"
if [ -z "$HEAD_SHA" ]; then
    echo "check-pr-analysis-surfaces: could not resolve PR #${PR}'s head - CANNOT RUN." >&2
    exit 2
fi
echo "check-pr-analysis-surfaces: head ${HEAD_SHA:0:9}"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# --- the changed-file set -------------------------------------------------------------------------
if ! gh pr view "$PR" --repo "$REPO" --json files -q '.files[].path' > "$WORK/changed.txt" 2>/dev/null; then
    echo "check-pr-analysis-surfaces: could not list PR #${PR}'s files - CANNOT RUN." >&2
    exit 2
fi
CHANGED=$(wc -l < "$WORK/changed.txt" | tr -d ' ')
if [ "$CHANGED" -eq 0 ]; then
    echo "check-pr-analysis-surfaces: PR #${PR} changes no files - CANNOT RUN (that is not a clean PR)." >&2
    exit 2
fi
echo "check-pr-analysis-surfaces: ${CHANGED} changed file(s)"

# THE LINES, not just the files. "A finding in a file you opened" and "a finding on a line you wrote"
# are different obligations, and collapsing them is what makes this kind of report ignorable: a PR
# that adds one helper to a 1800-line class inherits every pre-existing finding in it, the list runs
# to dozens, and the two findings that are actually yours are lost in it. The added-line ranges come
# from the diff's own hunk headers.
if ! gh pr diff "$PR" --repo "$REPO" > "$WORK/diff.txt" 2>/dev/null; then
    echo "check-pr-analysis-surfaces: could not read PR #${PR}'s diff - CANNOT RUN." >&2
    exit 2
fi

# --- surface 1: check-run annotations (SpotBugs, and javac via setup-java's problem matcher) --------
if ! gh api "repos/${REPO}/commits/${HEAD_SHA}/check-runs?per_page=100" > "$WORK/runs.json" 2>/dev/null; then
    echo "check-pr-analysis-surfaces: could not read check runs - CANNOT RUN." >&2
    exit 2
fi

# Annotations page at 100. A truncated fetch under-reports, which is the one failure mode this script
# must not have, so every page is followed rather than assuming one is enough.
: > "$WORK/ann.json"
while read -r id name; do
    [ -n "$id" ] || continue
    for page in 1 2 3 4 5; do
        if ! gh api "repos/${REPO}/check-runs/${id}/annotations?per_page=100&page=${page}" > "$WORK/p.json" 2>/dev/null; then
            break
        fi
        count=$(python3 -c "import json,sys; print(len(json.load(open(sys.argv[1]))))" "$WORK/p.json" 2>/dev/null || echo 0)
        [ "$count" -gt 0 ] || break
        python3 -c "
import json,sys
for a in json.load(open(sys.argv[1])):
    a['check_name'] = sys.argv[2]
    print(json.dumps(a))
" "$WORK/p.json" "$name" >> "$WORK/ann.json"
        if [ "$count" -eq 100 ] && [ "$page" -eq 5 ]; then
            # SILENT TRUNCATION IS THE ONE THING THIS SCRIPT MUST NOT DO. 5 pages of 100 is the cap,
            # and this repo already sees 601 findings unfiltered across the reactor - well inside the
            # range where a check run's annotations reach it. Under-reporting without saying so would
            # make "no findings on your diff" mean "no findings in the first 500", which is the
            # false-clean this tool exists to prevent.
            echo "check-pr-analysis-surfaces: WARNING - ${name} has more than 500 annotations." >&2
            echo "  Only the first 500 were read, so findings on your diff may be MISSING from the" >&2
            echo "  report below. Treat a clean result from this run as unproven." >&2
        fi
        [ "$count" -eq 100 ] || break
    done
done < <(python3 -c "
import json
for r in json.load(open('$WORK/runs.json'))['check_runs']:
    if (r.get('output') or {}).get('annotations_count', 0) > 0:
        print(r['id'], r['name'])
")

python3 - "$WORK/changed.txt" "$WORK/ann.json" "$WORK/diff.txt" <<'PYEOF'
import json, sys, collections, re

changed = {line.strip() for line in open(sys.argv[1]) if line.strip()}

# Added-line numbers per file, straight from the unified diff. Only '+' lines count: a finding on a
# context line is pre-existing code the PR merely sat next to.
added = collections.defaultdict(set)
cur = None
newno = 0
for raw in open(sys.argv[3], errors='replace'):
    if raw.startswith('+++ b/'):
        cur = raw[6:].rstrip('\n')
        continue
    m = re.match(r'@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@', raw)
    if m:
        newno = int(m.group(1))
        continue
    if cur is None:
        continue
    if raw.startswith('+'):
        added[cur].add(newno)
        newno += 1
    elif raw.startswith('-'):
        pass
    elif raw.startswith(' '):
        newno += 1
rows = []
try:
    for line in open(sys.argv[2]):
        line = line.strip()
        if line:
            rows.append(json.loads(line))
except FileNotFoundError:
    rows = []

# De-duplicate across check runs. The SAME javac warning is annotated once per compiling job, because
# every job that runs setup-java registers its javac problem matcher - so one warning can appear
# three or four times wearing different job names. Counting those separately would overstate the
# finding count several-fold and bury the distinct ones.
by_key = collections.OrderedDict()
# start_line ALONE is not enough to decide whose finding it is. Annotations carry an end_line too,
# and a SpotBugs finding that spans a method can start on an untouched line while ending inside one
# the PR added - classifying that as "inherited" is a false negative in the direction that matters,
# because it is the author's own change that provoked it. The span is kept and any overlap counts.
spans = {}
for a in rows:
    if a.get('path') not in changed:
        continue
    key = (a.get('path'), a.get('start_line'), (a.get('message') or '').strip())
    by_key.setdefault(key, []).append(a.get('check_name'))
    spans[key] = (a.get('start_line'), a.get('end_line') or a.get('start_line'))

def render(title, subtitle, items):
    print()
    print('=' * 96)
    print(title)
    print('=' * 96)
    print('  ' + subtitle)
    if not items:
        print('  none')
        return
    per_file = collections.defaultdict(list)
    for (path, line, msg), names in items.items():
        per_file[path].append((line, msg, sorted(set(names))))
    print('  %d distinct finding(s) across %d file(s)' % (len(items), len(per_file)))
    for path in sorted(per_file):
        print()
        print('  %s' % path)
        for line, msg, names in sorted(per_file[path], key=lambda x: (x[0] or 0)):
            short = ' '.join(msg.split())
            if len(short) > 150:
                short = short[:147] + '...'
            print('    line %-6s %s' % (line, short))
            print('    %-11s reported by: %s' % ('', ', '.join(names)))

yours, inherited = collections.OrderedDict(), collections.OrderedDict()
for key, names in by_key.items():
    path, line, _ = key
    lo, hi = spans.get(key, (line, line))
    try:
        span = range(int(lo), int(hi) + 1)
    except (TypeError, ValueError):
        span = [line]
    touched = added.get(path, ())
    mine = any(n in touched for n in span)
    (yours if mine else inherited)[key] = names

render('ON LINES THIS PR WROTE - these are yours',
       'A finding here is on a line in your diff. Fix it, or say in the PR why not.',
       yours)
render('IN FILES THIS PR TOUCHES, on lines it did not write - inherited',
       'Context, not an obligation. Do NOT bulk-fix these in this PR; they belong to the '
       'registries under docs/inflight/.',
       inherited)

# Only the lines this PR wrote fail the check. Inheriting a file's history is not a defect, and a
# gate that says otherwise would make every small PR to a large class unmergeable - which is how a
# check gets switched off rather than obeyed.
sys.exit(1 if yours else 0)
PYEOF
ANN_RC=$?

# --- surface 2: bot comments on the PR --------------------------------------------------------------
echo
echo "================================================================================================"
echo "BOT COMMENTS ON THIS PR - read these, they carry counts the annotations do not"
echo "================================================================================================"
gh api "repos/${REPO}/issues/${PR}/comments?per_page=100" --jq \
    '.[] | select(.user.type == "Bot" or .user.login == "github-actions") | "  \(.created_at)  \(.user.login)\n    \(.body | split("\n")[0:2] | join(" ") | .[0:150])"' \
    2>/dev/null || echo "  (could not read comments)"

# --- surface 3: what this script cannot reach --------------------------------------------------------
echo
echo "================================================================================================"
echo "SURFACES THIS SCRIPT CANNOT READ - open these by hand"
echo "================================================================================================"
echo "  Job summaries (PIT survivor table, CVE tables) are not exposed by the REST API."
gh api "repos/${REPO}/commits/${HEAD_SHA}/check-runs?per_page=100" \
    --jq '.check_runs[] | select(.name | test("Mutation|spotbugs|racerd|CVE|Quarantine")) | "    \(.name): \(.html_url)"' \
    2>/dev/null | sort -u || true
echo
echo "  Console-only output: a tool that prints to the Maven log and does not annotate is invisible"
echo "  here. Read the job log for those."

echo
if [ "$ANN_RC" -ne 0 ]; then
    echo "check-pr-analysis-surfaces: findings ON LINES THIS PR WROTE (above)."
    echo "  Fix them, or state in the PR why each stands. Inherited findings are listed separately"
    echo "  and are NOT what this exit code is about."
    exit 1
fi
echo "check-pr-analysis-surfaces: no annotations on the files this PR changes."
echo "  NOTE: that is not 'the analysis is clean' - it is 'nothing landed on your diff'. Findings"
echo "  elsewhere in the tree belong to the registries under docs/inflight/."
exit 0
