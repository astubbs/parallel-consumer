<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Documentation data

These YAML files are the canonical, renderer-independent data for release documentation. A future docs-site
renderer can turn them into pages, tables or navigation without changing their meaning. They are not generated.
They are deliberately rich enough to support a skeptical reader, not merely labels for a renderer.

Each file declares a `schema_version` and a `kind`; their contract lives in `schema.yaml`. Keep values structured
where the site will need to filter, group or order them. Use prose fields only for the short statements a reader
must see verbatim, and attach a repository path, command, test class or public URL to every important claim.

- `module-maturity.yaml` separates reliability confidence from API stability for every published module.
- `roadmap.yaml` describes themes, their order and the exit criteria for 1.0 without dates or issue-by-issue status.
- `testing-evidence.yaml` records executable test evidence, negative controls and investigation records.

Feature definitions live in `docs/features/`, one YAML file per user-visible feature. That avoids a shared index
file becoming a merge-conflict point for feature PRs. A definition must describe a use case, selection criteria,
setup, boundaries and evidence or references; `docs/features/README.md` records the exact contract.
