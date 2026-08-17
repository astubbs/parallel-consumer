# Polyglot demo app — ideation done, direction not yet chosen

Branch `docs/ideate-polyglot-demo` (off `feats/proxy-requirements`, astubbs#242) carries Part 2 of
[`docs/ideation/2026-08-14-language-proxy-interaction-model-ideation.html`](../ideation/2026-08-14-language-proxy-interaction-model-ideation.html):
seven ranked directions (six verifier-checked; the seventh, owner-added 2026-08-18) for a demo that
runs all eleven language bindings at once and proves in one view that they read the same records. Read the doc before designing any demo,
orchestration, aggregation or perf-display work — it records what was rejected and why (notably:
audience-as-workload keynote mode violates the recorded no-visitor-input security posture).

What no command will tell you:

- **Coordination with the perf-comparison track is agreed and recorded in the doc's boundary note**:
  this track owns app/UI/live loop/marketing narrative; the perf track (sibling ideation doc
  `2026-08-17-perf-comparison-matrix-ideation.html`, drafted in a concurrent session, not yet on a
  branch) owns workload definitions, measurement semantics and the blessed-numbers pipeline. The R77
  stats stream and the demo's observation-receipts topic are to be **one record shape** at different
  aggregation levels; the concrete schema is the perf track's brainstorm/plan-stage decision and is
  still open. The demo transport must be able to run receipts-off so it cannot distort measured
  numbers.
- **Merge collision pending, deliberate**: the HTTP-strategy and native-bindings sessions extended
  the *same* ideation HTML file on their own branches in parallel. Whoever merges these branches
  reconciles the sibling Part 2s at the source — decide then whether it stays one multi-part doc or
  splits per topic.
- **Doc-hygiene follow-up**: `parked-sidecar-embeds-web-gui.md` cites "KTD5 of the proxy plan" for
  its dependency freeze, but the plan's KTD5 is configuration-is-code — one-line citation fix.

Next step when picked up: choose a survivor (1, the skateboard stack, is the fastest to something
running; 3, the bring-your-own-topic what-if machine, is the owner's own direction and the strongest
adoption artifact; 5, the live performance show, is the most fully specified) and take it through
brainstorm to a plan; the work would land as a stacked PR on astubbs#293.
