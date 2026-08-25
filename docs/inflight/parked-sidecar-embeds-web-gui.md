# Parked: the sidecar embeds the web dashboard, pre-v6

Out of the language-proxy plan's scope (astubbs#242), but wanted **before the v6 release**: the
sidecar should be able to embed the inbound web-GUI system — the embedded dashboard of
astubbs/parallel-consumer#268 (astubbs#215), currently an open PR on `feats/web-gui`.

## Why it fits unusually well

A sidecar is the one deployment where the dashboard's "see inside a running PC" pitch has no
competing UI at all: the application is in another language and cannot host its own view of PC's
state, so the JVM process is otherwise a black box to its operator. The observability bill the
strategy doc says comes with moving the queue into the client is at its largest exactly here.

## Constraints already on record

- **KTD5 of the proxy plan (and its predecessor's KD/KTD record): the proxy module lands depending
  on nothing from `feats/web-gui`.** The duplicate-code gate is a 5% absolute cap against a ~4.2%
  baseline, and copying that branch's serving code is the shape that exceeds it. Embedding therefore
  waits until BOTH are on trunk, and arrives as a dependency on the dashboard's module (or the
  extracted shared serving module those branches already anticipate) — never as copied code.
- The sidecar's listener posture (loopback-only default, the bind opt-in, the authority allowlist)
  must govern the dashboard's port too — one exposure story, not two.
- `docs/inflight/branch-language-proxy.md` already records the collision surface between the two
  branches (root pom, detector lists, AGENTS.md, NOTICE); whichever lands second resolves.

## When to pick it up

After the language-proxy fan-out's engine units and astubbs#268 have both merged to master, and
before v6 ships. It is an integration unit on the sidecar's lifecycle/packaging seam, not a change
to the interaction model.
