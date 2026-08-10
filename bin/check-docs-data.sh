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


def require_fields(container, required, path, where):
    """Report each declared-required field that is missing. Wrong type is a finding, not a skip."""
    if not required:
        return
    if not isinstance(container, dict):
        problems.append(f"{path}: {where} should be a mapping, found {type(container).__name__}")
        return
    for field in required:
        if container.get(field) is None:
            problems.append(f"{path}: {where} requires '{field}', which is missing or empty")


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
            require_fields(item, spec.get(required_key), path, label)
            check_closed_sets(item, spec, path, f"{label}.")

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
                ev_required = status_spec.get("evidence_required") or []
                if ev_required:
                    evidence = availability.get("evidence")
                    require_fields(evidence, ev_required, path, "availability.evidence")
                    if isinstance(evidence, dict):
                        declared = set(ev_required) | set(status_spec.get("evidence_optional") or [])
                        for field in sorted(set(evidence) - declared):
                            problems.append(
                                f"{path}: availability.evidence carries undeclared field '{field}' - "
                                f"add it to evidence_optional in {SCHEMA_PATH} or remove it"
                            )
                milestones = availability.get("milestones")
                if milestones is not None and not isinstance(milestones, list):
                    problems.append(f"{path}: availability.milestones should be a list")
                for milestone in milestones or []:
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
