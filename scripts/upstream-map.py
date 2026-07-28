#!/usr/bin/env python3
"""Validate and render the fork<->upstream tracking cache.

Usage:
  scripts/upstream-map.py validate      # parse, check schema + duplicate ids (exit 1 on error)
  scripts/upstream-map.py table         # human-readable summary table
  scripts/upstream-map.py refs          # every upstream issue/PR/related number tracked
  scripts/upstream-map.py show <id>     # shell-eval-able fields for one entry
  scripts/upstream-map.py meta          # shell-eval-able manifest-level fields (last_swept, repos)
  scripts/upstream-map.py tracked       # '<kind> <n> <recorded_status> <id>' per primary ref
  scripts/upstream-map.py posted-refs   # upstream numbers already commented on (idempotency)
  scripts/upstream-map.py todo          # outstanding actions across entries (from `todo:` fields)

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
REQUIRED = ["id", "group", "summary", "fork", "upstream", "forwarded"]
FORK_STATUS = {"none", "in-progress", "ready", "pr-open", "merged", "released",
               "superseded", "wontfix"}
UPSTREAM_STATUS = {"open", "closed", "merged", "mixed"}
GROUPS = {"rebalance-stability", "metrics-observability", "vertx",
          "java-baseline-kafka4", "features", "deps-security", "deps-major",
          "deps-routine", "build-tooling", "logging-ux", "release", "governance"}


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
    """Emit shell-eval-able KEY=value lines for one entry (used by upstream-backlink.sh)."""
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


def posted_refs(d):
    """Print upstream issue/PR numbers we've already commented on (from forwarded urls).

    Used for idempotency -- upstream-backlink.sh skips these so we never double-post.
    """
    seen = set()
    for e in d["entries"]:
        for fw in (e.get("forwarded") or []):
            url = fw.get("url")
            if not url:
                continue
            m = re.search(r"/(?:issues|pull)/(\d+)", url)
            if m:
                seen.add(m.group(1))
    for n in sorted(seen, key=int):
        print(n)


def todos(d):
    """List outstanding actions across all entries (from `todo:` fields)."""
    found = False
    for e in d["entries"]:
        items = e.get("todo")
        if items:
            found = True
            print(f"{e['id']} [{e['fork']['status']}]:")
            for it in items:
                print(f"  - {it}")
    if not found:
        print("(no outstanding todos)")


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
        print(f"OK: {len(d['entries'])} entries, no schema errors")
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
    elif cmd == "posted-refs":
        posted_refs(d)
    elif cmd == "todo":
        todos(d)
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
