<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Documentation data

These YAML files are the canonical, renderer-independent data for release documentation. A future docs-site
renderer can turn them into pages, tables or navigation without changing their meaning. They are not generated.
They are deliberately rich enough to support a skeptical reader, not merely labels for a renderer.

Each file declares a `schema_version` and a `kind`; their contract lives in `schema.yaml`. Keep values structured
where the site will need to filter, group or order them. Use prose fields only for the short statements a reader
must see verbatim, and attach a repository path, command, test class or public URL to every important claim.

- `module-maturity.yaml` separates reliability confidence from API stability and links every module to its own evidence.
- `roadmap.yaml` carries big-picture entries a reader can watch finish, the concerns each serves, what unblocks what, and the exit criteria for 1.0. No dates, and no issue-by-issue status.
- `testing-evidence.yaml` records executable test evidence, harness limits, quality controls, negative controls, investigation records and v6's all-known-defects release gate.

Feature definitions live in `docs/features/`, one YAML file per user-visible feature. That avoids a shared index
file becoming a merge-conflict point for feature PRs. A definition must describe a use case, selection criteria,
setup, boundaries and evidence or references; `docs/features/README.md` records the exact contract.

`bin/check-docs-data.sh` validates the structure of everything here and in `docs/features/`: that each file parses,
declares its kind, carries the fields that kind requires in `schema.yaml`, and points at README anchors that exist.
It runs on every pull request. It checks structure only and cannot tell whether a claim is true, which is why the
claims carry their own evidence rather than relying on a gate.
