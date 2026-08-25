# Parked, post-v6: say in the docs that this project is built with agentic compound engineering

The published documentation should carry an entry stating that this fork is developed using agentic
compound engineering, linking to the practice's own site. Owner's call, 2026-08-14; **post-v6**, so
it does not compete with release work.

## Why it is worth saying

It is true of nearly every commit in this fork's recent history, and it is unusual enough to be
information rather than decoration - a reader evaluating a community-maintained fork of an
unmaintained upstream is entitled to know how the work is actually produced. It also sets the
expectation that the conventions in `AGENTS.md`, the per-directory rules, and the gate scripts exist
because agents write here, which explains a great deal about the repo's shape that otherwise reads
as over-engineering.

## Before publishing, two things need settling

- **Confirm the canonical URL rather than guessing it.** The practice is packaged here as the
  `compound-engineering` plugin (see the plugin cache under `~/.claude/plugins/`, which carries its
  version), but a link in published docs must be the project's own canonical address, verified at
  the time of writing - not reconstructed from memory. Do not ship a plausible-looking URL.
- **Decide where it belongs.** Candidates: the README (most visible, also the most crowded), the
  contributing guide (where a reader is already asking how work happens here), or
  `docs/agent-harness.md` (which already explains the layers agents obey, and would gain the
  external context it currently lacks). One place, not three - this repo's rule against stating a
  fact twice applies.

## Related

`docs/agent-harness.md` owns how agent rules are enforced here; `AGENTS.md` owns the rules
themselves. Neither currently names the practice they came from.
