#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Validate the structure of the release-documentation data: docs/features/*.yaml and docs/data/*.yaml.
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


for path in sorted(glob.glob("docs/features/*.yaml") + glob.glob("docs/data/*.yaml")):
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

    require_fields(doc, spec.get("required"), path, "kind '%s'" % kind)
    check_closed_sets(doc, spec, path, "")
    # schema_version and kind identify the record rather than describing it, so they never need
    # declaring; some kinds list them under required and some do not.
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

for problem in problems:
    print(f"DATA: {problem}")

if problems:
    print(f"\ncheck-docs-data: {len(problems)} structural problem(s) across {checked} file(s)")
    sys.exit(1)

print(f"check-docs-data: {checked} file(s) structurally valid")
PYTHON
