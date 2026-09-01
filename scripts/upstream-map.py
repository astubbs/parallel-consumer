#!/usr/bin/env python3
# Copyright (C) 2026 Antony Stubbs and contributors

"""Validate and render the fork<->upstream tracking cache.

Usage:
  scripts/upstream-map.py validate      # parse, check schema + duplicate ids (exit 1 on error)
  scripts/upstream-map.py table         # human-readable summary table
  scripts/upstream-map.py refs          # every upstream issue/PR/related number tracked
  scripts/upstream-map.py show <id>     # shell-eval-able fields for one entry
  scripts/upstream-map.py meta          # shell-eval-able manifest-level fields (last_swept, repos)
  scripts/upstream-map.py tracked       # '<kind> <n> <recorded_status> <id>' per primary ref

The manifest (src/docs/development/upstream-map.yaml) is the source of truth for
the fork<->upstream mapping; see its header block for the schema.
"""
import sys
import os
import collections
import shlex
import re

try:
    import yaml
except ImportError:
    sys.exit("PyYAML required: pip install pyyaml")

HERE = os.path.dirname(os.path.abspath(__file__))
MANIFEST = os.path.join(HERE, "..", "src", "docs", "development", "upstream-map.yaml")
# `forwarded` was dropped in the 2026-08-05 slim: every upstream issue is now mirrored, so the
# durable record of a backlink is the comment itself (it carries a hidden marker) plus the
# mirror, not a field here that goes stale the moment anyone comments.
REQUIRED = ["id", "group", "summary", "fork", "upstream"]
FORK_STATUS = {"none", "in-progress", "ready", "pr-open", "merged", "released",
               "superseded", "wontfix"}
UPSTREAM_STATUS = {"open", "closed", "merged", "mixed"}
GROUPS = {"rebalance-stability", "metrics-observability", "vertx",
          "java-baseline-kafka4", "features", "deps-security", "deps-major",
          "deps-routine", "build-tooling", "logging-ux", "release", "governance"}

# `branch_accounting` is the ONLY branch record (see the manifest header). It is a different shape
# from `entries` - keyed by ref, not id - so it needs its own arm rather than reuse.
BRANCH_STATES = {"mirrored", "ours", "archived", "deleted"}
ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def load():
    with open(MANIFEST) as fh:
        return yaml.safe_load(fh)


def validate(d):
    errs = []
    entries = d.get("entries") or []
    ids = [e.get("id") for e in entries]
    for dup in [k for k, v in collections.Counter(ids).items() if v > 1]:
        errs.append(f"duplicate id: {dup}")
    for e in entries:
        eid = e.get("id", "<no-id>")
        for k in REQUIRED:
            if k not in e:
                errs.append(f"{eid}: missing field '{k}'")
        st = (e.get("fork") or {}).get("status")
        if st not in FORK_STATUS:
            errs.append(f"{eid}: bad fork.status '{st}' (allowed: {sorted(FORK_STATUS)})")
        ust = (e.get("upstream") or {}).get("status")
        if ust not in UPSTREAM_STATUS:
            errs.append(f"{eid}: bad upstream.status '{ust}' (allowed: {sorted(UPSTREAM_STATUS)})")
        grp = e.get("group")
        if grp not in GROUPS:
            errs.append(f"{eid}: bad group '{grp}' (allowed: {sorted(GROUPS)})")
        # `pr-open` with no PR number is unenforceable, not merely untidy.
        # .claude/hooks/check-upstream-map-merged.sh denies a merge only when the PR being merged
        # appears in fork.prs AND the entry still says pr-open. With prs empty, `N in []` is false
        # for every N, so the guard cannot fire on the entry that most needs it - and the state is
        # reachable in one step: split work onto a new branch, and the entry names a status whose
        # own PR does not exist yet. Caught here rather than left to the hook, which by design
        # stays silent when it has nothing to match.
        fork = e.get("fork") or {}
        if fork.get("status") == "pr-open" and not (fork.get("prs") or []):
            errs.append(
                f"{eid}: fork.status is 'pr-open' but fork.prs is empty - the merge guard matches on "
                f"the PR number, so it can never fire for this entry. Set fork.prs when the PR is "
                f"opened, or use 'ready' until then."
            )
    errs.extend(validate_branches(d))
    return errs


def validate_branches(d):
    """Check `branch_accounting`, which went unvalidated from the day it was added.

    It arrived in astubbs#327 alongside a `validate` that read only `entries`, so a bogus `state`
    passed silently while the run printed `OK: 39 entries` - a reassuring count of the OTHER
    section. That is the shape this repo keeps rediscovering: not a gate that fails, a gate that
    reports success about something it never looked at. The success line now names both sections
    for the same reason.
    """
    errs = []
    branches = d.get("branch_accounting") or []
    if not isinstance(branches, list):
        return [f"branch_accounting: should be a list, found {type(branches).__name__}"]
    refs_seen = [b.get("ref") for b in branches if isinstance(b, dict)]
    for dup in [k for k, v in collections.Counter(refs_seen).items() if v > 1]:
        errs.append(f"branch_accounting: duplicate ref: {dup}")
    for b in branches:
        if not isinstance(b, dict):
            errs.append(f"branch_accounting: entry is not a mapping: {b!r}")
            continue
        ref = b.get("ref") or "<no-ref>"
        if not b.get("ref"):
            errs.append(f"branch_accounting: entry missing 'ref': {b!r}")
        st = b.get("state")
        if st not in BRANCH_STATES:
            errs.append(f"{ref}: bad state '{st}' (allowed: {sorted(BRANCH_STATES)})")
        # A tip is REQUIRED only once the entry outlives the branch. For a live branch the SHA is
        # one `git rev-parse` away, and this section's governing rule is "nothing a command
        # answers" - so demanding it there would contradict the schema it validates. Once the
        # branch is deleted or archived nothing can answer it, and an entry recording that we
        # removed something without saying what is the exact failure the section exists to prevent.
        tip = b.get("tip")
        if tip is None:
            if st in ("deleted", "archived"):
                errs.append(f"{ref}: state '{st}' requires a 'tip' - nothing else can recover it")
        elif not isinstance(tip, str):
            # `tip: 255916684` is all digits, so YAML makes it an int and any string comparison
            # against `git rev-parse` silently never matches. Already hit once, in review.
            errs.append(f"{ref}: 'tip' parsed as {type(tip).__name__}, not str - quote it")
        if st == "deleted":
            when = b.get("deleted")
            if when is None:
                errs.append(f"{ref}: state 'deleted' requires a 'deleted' date")
            elif not ISO_DATE.match(str(when)):
                errs.append(f"{ref}: 'deleted' should be an ISO date (YYYY-MM-DD), found '{when}'")
        see = b.get("see")
        if see is not None and not isinstance(see, list):
            errs.append(f"{ref}: 'see' should be a list, found {type(see).__name__}")
    return errs


def table(d):
    print(f"last_swept={d['last_swept']}  upstream={d['upstream_repo']}  fork={d['fork_repo']}")
    hdr = f"{'id':38} {'fork_status':12} {'fork_prs':9} {'up_issues':16} {'up_prs':14} up_state"
    print(hdr)
    print("-" * len(hdr))
    for e in d["entries"]:
        f, u = e["fork"], e["upstream"]
        print(f"{e['id']:38} {str(f['status']):12} {str(f.get('prs') or ''):9} "
              f"{str(u.get('issues') or ''):16} {str(u.get('prs') or ''):14} {u.get('status')}")


def refs(d):
    """Emit 'issue <n>' / 'pr <n>' lines for every tracked upstream ref (dedup)."""
    seen = set()
    for e in d["entries"]:
        u = e["upstream"]
        for n in (u.get("issues") or []):
            seen.add(("issue", n))
        for n in (u.get("prs") or []):
            seen.add(("pr", n))
        for n in (u.get("related") or []):
            seen.add(("ref", n))
    for kind, n in sorted(seen, key=lambda x: (x[0], x[1])):
        print(f"{kind} {n}")


def show(d, eid):
    """Emit shell-eval-able KEY=value lines for one entry."""
    e = next((x for x in d["entries"] if x.get("id") == eid), None)
    if e is None:
        sys.exit(f"no entry with id '{eid}'")
    u, f = e["upstream"], e["fork"]
    q = shlex.quote

    def nums(v):
        return " ".join(str(n) for n in (v or []))

    print(f"ID={q(e['id'])}")
    print(f"SUMMARY={q(e['summary'])}")
    print(f"GROUP={q(e['group'])}")
    print(f"FORK_REPO={q(d['fork_repo'])}")
    print(f"FORK_STATUS={q(str(f['status']))}")
    print(f"FORK_PRS={q(nums(f.get('prs')))}")
    print(f"FORK_BRANCHES={q(' '.join(f.get('branches') or []))}")
    print(f"UPSTREAM_REPO={q(u['repo'])}")
    print(f"ISSUES={q(nums(u.get('issues')))}")
    print(f"PRS={q(nums(u.get('prs')))}")
    print(f"RELATED={q(nums(u.get('related')))}")
    fw = " ".join(x.get("url") for x in (e.get("forwarded") or []) if x.get("url"))
    print(f"FORWARDED={q(fw)}")
    print(f"BACKLINK={q(e.get('backlink') or '')}")


def meta(d):
    """Emit shell-eval-able manifest-level fields (used by upstream-sweep.sh)."""
    q = shlex.quote
    print(f"LAST_SWEPT={q(str(d.get('last_swept', '')))}")
    print(f"UPSTREAM_REPO={q(d['upstream_repo'])}")
    print(f"FORK_REPO={q(d['fork_repo'])}")


def tracked(d):
    """One line per tracked upstream issue/PR: '<kind> <number> <recorded_status> <entry_id>'.

    Only primary refs (issues/prs), not 'related' -- these are the ones whose
    state drift is worth flagging.
    """
    for e in d["entries"]:
        u = e["upstream"]
        st = u.get("status", "")
        for n in (u.get("issues") or []):
            print(f"issue {n} {st} {e['id']}")
        for n in (u.get("prs") or []):
            print(f"pr {n} {st} {e['id']}")


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "validate"
    d = load()
    if cmd == "validate":
        errs = validate(d)
        if errs:
            print("INVALID:")
            for e in errs:
                print("  " + e)
            sys.exit(1)
        print(f"OK: {len(d['entries'])} entries, "
              f"{len(d.get('branch_accounting') or [])} branch records, no schema errors")
    elif cmd == "table":
        table(d)
    elif cmd == "refs":
        refs(d)
    elif cmd == "show":
        if len(sys.argv) < 3:
            sys.exit("usage: upstream-map.py show <entry-id>")
        show(d, sys.argv[2])
    elif cmd == "meta":
        meta(d)
    elif cmd == "tracked":
        tracked(d)
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
