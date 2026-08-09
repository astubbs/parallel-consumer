---
title: Feature documentation
copyright: Copyright (C) 2026 Antony Stubbs and contributors
---

# Feature documentation

Each YAML file in this directory defines a user-visible capability: what it does, when to use it, how mature
it is and which constraints matter. They are not changelog fragments. A release note tells a reader that
something changed; a feature definition remains useful whenever the documentation site needs to explain the
capability.

New user-visible features add one file rather than editing a shared index. This keeps concurrent feature PRs
from colliding. The future documentation site can render these definitions directly; this branch only owns
the data, not that rendering.

The first set maps every capability named in the README's `Features List`, then extends that seed with later
user-visible capabilities found in the tagged release history: commit modes, retry and executor controls,
PollContext, metrics, recovery policies and buffer configuration. It also includes the v6 alpha modules. The README
stays as the current presentation until the separate rendering work consumes these files.

## Page contract

Use a YAML document with `schema_version: 1`, `kind: feature`, `title`, `category`, `module`, `maturity` and
`availability`. `availability` distinguishes a published capability (`since` is its first released tag) from planned
release content (`target_release`). Published values name their tag and the repository evidence used to establish it;
do not stamp an existing capability with the current release. The renderer-independent contract is in
`../data/schema.yaml`. Then cover:

1. the use case and what the feature does;
2. the module or artifact a reader must choose, with setup data;
3. important limits, failure modes, validation steps and opt-in boundaries; and
4. references to examples, configuration or tracking issues when they help a user act.

## Availability provenance

The release history, not the README, establishes `since`. Use the first released tag containing the implementation
commit, and record that tag, the commit where it can be identified and a concise evidence basis. A capability can
carry later `milestones` when a release materially expanded it; for example, basic throttling existed in `0.1.0`,
while the adaptive queue and offset-payload controls landed in `0.3.0.0`. Do not create a feature page for every bug
fix or refactor—create one when a user gains a new API, configuration choice, observable surface or supported
operational behavior.

The pull-request checklist asks authors to add feature documentation data or state why it is not applicable.
A refactor with no user-visible behavior can be N/A; a new option or consumable module normally cannot.
