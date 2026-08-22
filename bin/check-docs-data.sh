#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Validate the structure of the release-documentation data: docs/features/*.yaml, docs/data/*.yaml
# and the per-module fragments in docs/data/*.d/.
#
# WHY THIS EXISTS
#
# These files are hand-written and a documentation generator reads them. A malformed file, a missing
# required field or a dangling README anchor produces a broken generated page rather than a loud
# error, and the failure surfaces to a reader rather than to the author. This check is deliberately
# structural only: it verifies that a file parses, declares its kind, carries the fields that kind
# requires, and that any anchor it points at exists. It does NOT verify that the claims are true -
# nothing can, and pretending otherwise would be worse than not checking.
#
# It also cross-checks the data against the Maven reactor, in both directions:
#
#   - Every module in a <modules> list (the root pom's AND any nested aggregator's) must have a
#     module-maturity row or a recorded `deferred:` entry. A module with neither used to pass
#     clean, which is a silent gap: the maturity corpus claimed completeness it did not have.
#   - Every record naming a module must name one the reactor builds. The repo already carries the
#     scar of the other direction: two feature records published Maven coordinates that could not
#     resolve, and were removed rather than shipped (their successors live in docs/data/staging/,
#     which is deliberately outside this check).
#
# Per-module records may live as fragments - docs/data/module-maturity.d/<artifact>.yaml and
# docs/data/testing-evidence.d/<artifact>.yaml - merged into the root file's corpus here. One
# module, one file, one owner: the filename must match the artifact, so no two concurrent waves
# ever write the same data file, and a module keyed in two places fails loudly rather than one
# copy silently winning a merge. Deferrals live INSIDE a module's own fragment (`deferred:` with a
# reason and what lifts it), never on a shared list, and every current deferral is named in the
# output so none is quietly forgotten.
#
# Exits non-zero on any structural problem. Run locally before pushing, and on every PR.

set -euo pipefail

cd "$(dirname "$0")/.."

PY=""
for c in python3 python; do
  if command -v "$c" >/dev/null 2>&1 && "$c" -c '' >/dev/null 2>&1; then PY="$c"; break; fi
done
if [ -z "$PY" ]; then
  echo "check-docs-data: no working Python 3 on PATH" >&2
  exit 2
fi

if ! "$PY" -c 'import yaml' >/dev/null 2>&1; then
  echo "check-docs-data: PyYAML is not installed. Install it with: $PY -m pip install pyyaml" >&2
  exit 2
fi

"$PY" - "$@" <<'PYTHON'
import glob
import os
import re
import sys

import yaml

SCHEMA_PATH = "docs/data/schema.yaml"
TEMPLATE_PATH = "src/docs/README_TEMPLATE.adoc"
README_TARGETS = {"README.adoc", "src/docs/README_TEMPLATE.adoc"}
PATH_SUFFIXES = (".yaml", ".yml", ".md", ".adoc", ".sh", ".java")

problems = []
checked = 0


def load(path):
    """Parse a YAML file, recording a problem rather than raising."""
    try:
        with open(path, encoding="utf-8") as handle:
            return yaml.safe_load(handle)
    except Exception as exc:  # noqa: BLE001 - any parse failure is a finding, not a crash
        problems.append(f"{path}: does not parse as YAML - {exc}")
        return None


schema = load(SCHEMA_PATH)
if schema is None:
    print("check-docs-data: cannot read the schema, so nothing can be validated", file=sys.stderr)
    sys.exit(1)

kinds = schema.get("kinds") or {}
if not kinds:
    problems.append(f"{SCHEMA_PATH}: declares no kinds, so no file can be validated against it")

# A kind that supports fragments names the collection a fragment carries. That name must be one an
# item contract governs, or every fragment's single entry would be read and never validated - the
# same silent-enforcement defect the item_contracts loop below guards against.
for kind_name, kind_spec in kinds.items():
    if not isinstance(kind_spec, dict):
        continue
    fragment_key = kind_spec.get("fragment_collection")
    if fragment_key is not None and fragment_key not in (kind_spec.get("item_contracts") or {}).values():
        problems.append(
            f"{SCHEMA_PATH}: '{kind_name}.fragment_collection' names '{fragment_key}', which no item "
            f"contract governs - fragment entries would go unvalidated"
        )


def fragment_root(path):
    """docs/data/module-maturity.d/foo.yaml -> docs/data/module-maturity.yaml; None for non-fragments."""
    parent = os.path.dirname(path)
    return parent[: -len(".d")] + ".yaml" if parent.endswith(".d") else None

anchors = set()
if os.path.exists(TEMPLATE_PATH):
    with open(TEMPLATE_PATH, encoding="utf-8") as handle:
        anchors = set(re.findall(r"^\[\[([^\]]+)\]\]", handle.read(), re.MULTILINE))
else:
    problems.append(f"{TEMPLATE_PATH}: missing, so README anchors cannot be checked")


def is_empty(value):
    """Missing, or present but carrying nothing.

    `title: ""`, `boundaries: []` and `evidence: {}` all satisfy a naive `is None` test while saying
    nothing, so a required field could be emptied out by a bad edit and still be reported valid. A
    boolean or a number is never empty: `blocks_1_0: false` is an answer, not an absence.
    """
    if value is None:
        return True
    if isinstance(value, (bool, int, float)):
        return False
    return hasattr(value, "__len__") and len(value) == 0


def require_fields(container, required, path, where):
    """Report each declared-required field that is missing. Wrong type is a finding, not a skip."""
    if not required:
        return
    if not isinstance(container, dict):
        problems.append(f"{path}: {where} should be a mapping, found {type(container).__name__}")
        return
    for field in required:
        if is_empty(container.get(field)):
            problems.append(f"{path}: {where} requires '{field}', which is missing or empty")


def check_declared_fields(container, required, optional, path, where, escape_hatch):
    """Close a field set against what the schema declares, but only where it declares an optional list.

    Declaring an `optional` list is how a level opts in to a closed set: it is the schema saying
    "these are the extras that may appear". Without enforcement that list is documentation nobody can
    trust, which is the same defect as a per-item contract governing no collection. A level that
    declares no optional list is making no such claim, so nothing is closed there.
    """
    if optional is None or not isinstance(container, dict):
        return
    declared = set(required or []) | set(optional)
    for field in sorted(set(container) - declared):
        problems.append(
            f"{path}: {where} carries undeclared field '{field}' - "
            f"add it to {escape_hatch} in {SCHEMA_PATH} or remove it"
        )


def check_closed_sets(container, spec, path, where):
    """Closed value sets, declared as <field>_values. Applies at any nesting level."""
    if not isinstance(container, dict):
        return
    for field in ("category", "maturity"):
        allowed = spec.get(f"{field}_values")
        value = container.get(field)
        if allowed and value is not None and value not in allowed:
            problems.append(
                f"{path}: {where}{field} '{value}' is not in the closed set "
                f"({', '.join(sorted(allowed))}). Widen it in {SCHEMA_PATH} if it genuinely needs it"
            )


def check_refs(node, path, where):
    """Resolve anything path-shaped, wherever it appears - including inside prose.

    A reference written as prose ("... see ordering-modes.yaml.") is the corpus's dominant style, so
    matching only whole-string paths misses most of them. Tokens are extracted from within the
    string instead.
    """
    if isinstance(node, dict):
        for key, value in node.items():
            check_refs(value, path, f"{where}.{key}" if where else key)
        return
    if isinstance(node, list):
        for item in node:
            check_refs(item, path, where)
        return
    if not isinstance(node, str):
        return
    for raw in re.split(r"[\s,;()\[\]<>\"\']+", node):
        token = raw.rstrip(".").strip()
        if not token or token.startswith(("http://", "https://")):
            continue
        target, _, fragment = token.partition("#")
        if not target.endswith(PATH_SUFFIXES):
            continue
        here = os.path.dirname(path)
        resolved = None
        for candidate in (os.path.normpath(os.path.join(here, target)), target):
            if os.path.exists(candidate):
                resolved = candidate
                break
        if resolved is None:
            problems.append(f"{path}: {where} points at '{target}', which does not exist")
            continue
        # A fragment is only checkable where anchors are declarable, which here means the README and
        # its template. Records also cite Java-source anchors such as
        # ParallelConsumerOptions.java#transactionalJavadoc; the file is verified, the fragment after
        # it deliberately is not, because Java declares no such anchor for anything to check against.
        # Read this as a stated boundary, not an oversight: a stale fragment on a non-README target
        # will not be caught.
        if fragment and resolved in README_TARGETS and anchors and fragment not in anchors:
            problems.append(
                f"{path}: {where} points at '{target}#{fragment}', "
                f"but no [[{fragment}]] exists in {TEMPLATE_PATH}"
            )


# Cross-check inputs, collected as files are validated and reconciled against the reactor at the
# end. Each record: module key, source file, whether it is a deferral, and what it points at.
maturity_records = []
evidence_records = []
feature_modules = []

for path in sorted(glob.glob("docs/features/*.yaml") + glob.glob("docs/data/*.yaml")
                   + glob.glob("docs/data/*.d/*.yaml")):
    if os.path.basename(path) == os.path.basename(SCHEMA_PATH):
        continue
    doc = load(path)
    if doc is None:
        continue
    checked += 1

    if not isinstance(doc, dict):
        problems.append(f"{path}: top level is not a mapping")
        continue

    kind = doc.get("kind")
    if kind is None:
        problems.append(f"{path}: no kind")
        continue
    spec = kinds.get(kind)
    if spec is None:
        problems.append(f"{path}: kind '{kind}' is not declared in {SCHEMA_PATH}")
        continue

    root_file = fragment_root(path)
    is_fragment = root_file is not None
    if is_fragment:
        # A fragment is one module's record and nothing shared: same kind, same row schema, merged
        # into the root file's corpus. The root file's preamble fields (release, axes, reader
        # contract...) are repo-level, so the file-level required list does not apply here.
        expected_kind = os.path.basename(root_file)[: -len(".yaml")]
        if kind != expected_kind:
            problems.append(
                f"{path}: a fragment in {os.path.dirname(path)}/ must carry kind "
                f"'{expected_kind}', found '{kind}'"
            )
            continue
        collection_key = spec.get("fragment_collection")
        if collection_key is None:
            problems.append(
                f"{path}: kind '{kind}' declares no fragment_collection in {SCHEMA_PATH}, "
                f"so it cannot be split into fragments"
            )
            continue
        check_declared_fields(doc, {collection_key, "schema_version", "kind"}, [],
                              path, "fragment top level", f"'{kind}.fragment_collection'")
        entries = doc.get(collection_key)
        if not isinstance(entries, list) or len(entries) != 1:
            problems.append(
                f"{path}: a fragment carries exactly one '{collection_key}' entry - "
                f"one module, one file, one owner"
            )
        else:
            # The filename IS the ownership claim: it is what guarantees two waves cannot write
            # the same module's data without git itself refusing, so a mismatch is an error even
            # when the content is otherwise valid.
            stem = os.path.basename(path)[: -len(".yaml")]
            key = entries[0].get("artifact") if isinstance(entries[0], dict) else None
            if key != stem:
                problems.append(
                    f"{path}: names artifact '{key}' but the file is named '{stem}.yaml' - "
                    f"the filename is the ownership claim, so they must match"
                )
    else:
        require_fields(doc, spec.get("required"), path, "kind '%s'" % kind)
    check_closed_sets(doc, spec, path, "")
    # schema_version and kind identify the record rather than describing it, so they never need
    # declaring; some kinds list them under required and some do not.
    if not is_fragment:
        check_declared_fields(
            doc,
            set(spec.get("required") or []) | {"schema_version", "kind"},
            spec.get("optional"),
            path, f"kind '{kind}'", f"'{kind}.optional'",
        )

    # An optional list with no required partner governs nothing, the mirror of the item_contracts
    # check below. Both are the same failure: a declaration everyone reads as enforced that is not.
    for optional_key in [k for k in spec if k.endswith("_optional")]:
        partner = optional_key[: -len("_optional")] + "_required"
        if partner not in spec:
            problems.append(
                f"{SCHEMA_PATH}: '{kind}.{optional_key}' has no '{kind}.{partner}' to extend, so it "
                f"closes nothing"
            )

    # Nested per-item contracts. The schema declares which collection each one governs, so a
    # contract it declares can never sit unenforced without this loop failing loudly.
    contracts = spec.get("item_contracts") or {}
    for required_key in [k for k in spec if k.endswith("_required") and k != "required"]:
        collection_key = contracts.get(required_key)
        if collection_key is None:
            problems.append(
                f"{SCHEMA_PATH}: '{kind}.{required_key}' governs no collection - add it to "
                f"'{kind}.item_contracts' or the contract is unenforced"
            )
            continue
        items = doc.get(collection_key)
        if items is None:
            continue
        if not isinstance(items, list):
            problems.append(f"{path}: '{collection_key}' should be a list of items")
            continue
        for index, item in enumerate(items):
            label = f"{collection_key}[{index}]"
            # A deferred entry replaces the row: `artifact` plus `deferred:` saying why no claim is
            # published and what lifts it, and nothing else. Only the fragment collection may carry
            # one, and only in a fragment - a deferral in the shared root file is exactly the
            # shared-list shape the fragment split exists to remove.
            if (isinstance(item, dict) and "deferred" in item
                    and collection_key == spec.get("fragment_collection")):
                if not is_fragment:
                    problems.append(
                        f"{path}: {label} is deferred, but deferral lives in the module's own "
                        f"fragment (docs/data/{kind}.d/<artifact>.yaml), never in the shared "
                        f"root file"
                    )
                deferral_spec = spec.get("deferral") or {}
                require_fields(item, ["artifact", "deferred"], path, label)
                check_declared_fields(item, ["artifact", "deferred"], [], path, label,
                                      f"'{kind}.deferral'")
                deferred = item.get("deferred")
                if isinstance(deferred, dict):
                    require_fields(deferred, deferral_spec.get("required"), path,
                                   f"{label}.deferred")
                    check_declared_fields(deferred, deferral_spec.get("required"),
                                          deferral_spec.get("optional") or [], path,
                                          f"{label}.deferred", f"'{kind}.deferral'")
                elif deferred is not None:
                    problems.append(
                        f"{path}: {label}.deferred should be a mapping, "
                        f"found {type(deferred).__name__}"
                    )
                continue
            optional_key = required_key[: -len("_required")] + "_optional"
            require_fields(item, spec.get(required_key), path, label)
            check_closed_sets(item, spec, path, f"{label}.")
            check_declared_fields(item, spec.get(required_key), spec.get(optional_key),
                                  path, label, f"'{kind}.{optional_key}'")

    availability = doc.get("availability")
    av_spec = spec.get("availability")
    if av_spec:
        if availability is None:
            pass  # already reported by the required-field check when it is required
        elif not isinstance(availability, dict):
            problems.append(
                f"{path}: availability should be a mapping, found {type(availability).__name__}"
            )
        else:
            status = availability.get("status")
            status_spec = av_spec.get(status) if status else None
            if status is None:
                problems.append(f"{path}: availability has no status")
            elif status_spec is None:
                problems.append(
                    f"{path}: availability status '{status}' is not declared in {SCHEMA_PATH} "
                    f"({', '.join(sorted(av_spec))})"
                )
            else:
                require_fields(availability, status_spec.get("required"), path,
                               f"availability status '{status}'")
                check_declared_fields(availability, status_spec.get("required"),
                                      status_spec.get("optional"), path,
                                      f"availability status '{status}'",
                                      f"'{kind}.availability.{status}.optional'")
                ev_required = status_spec.get("evidence_required") or []
                if ev_required:
                    evidence = availability.get("evidence")
                    require_fields(evidence, ev_required, path, "availability.evidence")
                    check_declared_fields(evidence, ev_required,
                                          status_spec.get("evidence_optional"), path,
                                          "availability.evidence",
                                          f"'{kind}.availability.{status}.evidence_optional'")
                milestones = availability.get("milestones")
                if milestones is None:
                    pass
                elif not isinstance(milestones, list):
                    # Report and stop. Falling through iterates a string character by character and
                    # emits one problem per character, burying the real finding in noise.
                    problems.append(
                        f"{path}: availability.milestones should be a list, "
                        f"found {type(milestones).__name__}"
                    )
                else:
                    for milestone in milestones:
                        require_fields(milestone, status_spec.get("milestone_required"), path,
                                       "a milestone")

    anchor = doc.get("readme_anchor")
    if anchor and anchors and anchor not in anchors:
        problems.append(f"{path}: readme_anchor '{anchor}' has no [[{anchor}]] in {TEMPLATE_PATH}")

    check_refs(doc, path, "")

    # Collect the module-keyed records for the reactor cross-checks below.
    if kind in ("module-maturity", "testing-evidence"):
        record_collection = "modules" if kind == "module-maturity" else "module_evidence"
        entries = doc.get(record_collection)
        if isinstance(entries, list):
            for item in entries:
                if not isinstance(item, dict):
                    continue
                record = {
                    "kind": kind,
                    "path": path,
                    "artifact": item.get("artifact"),
                    "id": item.get("id"),
                    "evidence_id": item.get("evidence_id"),
                    "is_deferred": "deferred" in item,
                    "deferred": item.get("deferred"),
                }
                (maturity_records if kind == "module-maturity" else evidence_records).append(record)
    elif kind == "feature":
        module = doc.get("module")
        if isinstance(module, str) and module:
            feature_modules.append((module, path))

# ---------------------------------------------------------------------------
# Reactor cross-checks. Everything above validates each file against the schema; nothing in it
# notices a module the data never mentions, and a module with no row used to pass clean. So the
# maturity corpus is reconciled against the Maven reactor here, in both directions, and merged
# corpora (root file plus fragments) are checked for the failure mode the merge itself introduces:
# the same module keyed twice, where without this one copy would silently win.
# ---------------------------------------------------------------------------


def declared_modules(pom_path):
    """<module> entries of one pom, with XML comments stripped so a commented-out module is absent."""
    try:
        with open(pom_path, encoding="utf-8") as handle:
            text = handle.read()
    except OSError as exc:
        problems.append(f"{pom_path}: cannot be read for the module cross-check - {exc}")
        return []
    text = re.sub(r"<!--.*?-->", "", text, flags=re.S)
    return re.findall(r"<module>\s*([^<]+?)\s*</module>", text)


# Walk every aggregator, not just the root pom: a module declared only in a nested aggregator's
# <modules> (the examples tree today, the client tree tomorrow) is still a module the reactor
# builds, and skipping it would reopen the silent gap one level down.
reactor = {}  # module directory basename -> the pom that declares it
pom_queue, poms_seen = [("pom.xml", "")], set()
while pom_queue:
    pom_path, base_dir = pom_queue.pop()
    if pom_path in poms_seen:
        continue
    poms_seen.add(pom_path)
    for rel in declared_modules(pom_path):
        module_dir = os.path.normpath(os.path.join(base_dir, rel))
        reactor.setdefault(os.path.basename(module_dir), pom_path)
        child_pom = os.path.join(module_dir, "pom.xml")
        if os.path.exists(child_pom):
            pom_queue.append((child_pom, module_dir))

# Duplicate module keys across the merged corpus - fragments plus root file. Also duplicate
# module_evidence ids, because evidence_id resolves by id, so two entries sharing one would leave
# a maturity row pointing at whichever the reader happened to take.
for records, corpus in ((maturity_records, "module-maturity"), (evidence_records, "testing-evidence")):
    by_artifact = {}
    for record in records:
        if record["artifact"]:
            by_artifact.setdefault(record["artifact"], []).append(record["path"])
    for artifact, paths in sorted(by_artifact.items()):
        if len(paths) > 1:
            problems.append(
                f"duplicate {corpus} record for module '{artifact}' in {' and '.join(paths)} - "
                f"a module's record lives in exactly one file"
            )
by_id = {}
for record in evidence_records:
    if record["id"]:
        by_id.setdefault(record["id"], []).append(record["path"])
for evidence_id, paths in sorted(by_id.items()):
    if len(paths) > 1:
        problems.append(
            f"duplicate module_evidence id '{evidence_id}' in {' and '.join(paths)} - "
            f"evidence_id resolution needs exactly one target"
        )

# Forward: every reactor module has a maturity row or a recorded deferral. Omission is the error
# being removed here; deliberate deferral stays possible, and stays visible via the DEFERRED lines
# printed below on every run.
maturity_covered = {record["artifact"] for record in maturity_records if record["artifact"]}
for module_name, declaring_pom in sorted(reactor.items()):
    if module_name not in maturity_covered:
        problems.append(
            f"{declaring_pom}: module '{module_name}' has no module-maturity row and no recorded "
            f"deferral - add docs/data/module-maturity.d/{module_name}.yaml carrying its row, or "
            f"'deferred:' with a reason and what lifts it"
        )

# Reverse: a maturity record or a feature record naming a module the reactor does not build is the
# shape of the scar this repo already carries - published coordinates that could not resolve.
# module_evidence entries are deliberately NOT reverse-checked: the streams-alpha and connect-alpha
# entries describe planned modules on purpose, as the evidence targets the staged rows in
# docs/data/staging/ point at, and the maturity corpus is the surface that claims a module exists.
for record in maturity_records:
    if record["artifact"] and record["artifact"] not in reactor:
        problems.append(
            f"{record['path']}: names module '{record['artifact']}', which is in no pom.xml "
            f"<modules> list - published coordinates for a module the reactor does not build "
            f"cannot resolve"
        )
for module, path in feature_modules:
    if module not in reactor:
        problems.append(
            f"{path}: module '{module}' is in no pom.xml <modules> list - a feature record must "
            f"name a module the reactor builds"
        )

# Every non-deferred maturity row's evidence_id must resolve to a module_evidence id somewhere in
# the merged testing-evidence corpus. A deferred evidence entry carries no id, so it deliberately
# satisfies nothing.
known_evidence_ids = {record["id"] for record in evidence_records
                      if record["id"] and not record["is_deferred"]}
for record in maturity_records:
    if not record["is_deferred"] and record["evidence_id"] \
            and record["evidence_id"] not in known_evidence_ids:
        problems.append(
            f"{record['path']}: evidence_id '{record['evidence_id']}' matches no module_evidence "
            f"id in the testing-evidence corpus"
        )

# Name every current deferral, on every run. A deferral that only surfaced on failure would be
# forgotten precisely while everything is green, which is how a deferral becomes an omission.
for record in sorted(maturity_records + evidence_records,
                     key=lambda r: (r["kind"], r["artifact"] or "")):
    if record["is_deferred"]:
        reason = record["deferred"].get("reason") if isinstance(record["deferred"], dict) else None
        print(f"DEFERRED: {record['kind']} for {record['artifact']} - "
              f"{reason or 'no reason recorded'} ({record['path']})")

for problem in problems:
    print(f"DATA: {problem}")

if problems:
    print(f"\ncheck-docs-data: {len(problems)} structural problem(s) across {checked} file(s)")
    sys.exit(1)

print(f"check-docs-data: {checked} file(s) structurally valid")
PYTHON
