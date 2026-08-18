# Polyglot demo app — ideation done, direction not yet chosen

Branch `docs/ideate-polyglot-demo` (off `feats/proxy-requirements`, astubbs#242) carries the
polyglot-demo continuation (ideas 21–27) of
[`docs/ideation/2026-08-14-language-proxy-interaction-model-ideation.html`](../ideation/2026-08-14-language-proxy-interaction-model-ideation.html):
seven ranked directions (six verifier-checked; the seventh, owner-added 2026-08-18) for a demo that
runs all eleven language bindings at once and proves in one view that they read the same records.
Read the doc before designing any demo, orchestration, aggregation or perf-display work — it records
what was rejected and why (notably: audience-as-workload keynote mode violates the recorded
no-visitor-input security posture).

What no command will tell you:

- **Coordination with the perf-comparison track is agreed and recorded in both docs**: this track
  owns app/UI/live loop/marketing narrative; the perf track
  ([`next-perf-comparison-matrix.md`](next-perf-comparison-matrix.md)) owns workload definitions,
  measurement semantics and the blessed-numbers pipeline. The R77 stats stream and the demo's
  observation-receipts topic are to be **one record shape** at different aggregation levels; the
  perf track's scenario definitions parameterize delay, ordering, failure percentage and
  concurrency so the what-if machine (idea 23) and the matrix run one definition. The concrete
  schema is the perf track's brainstorm/plan-stage decision and is still open. The demo transport
  must be able to run receipts-off, and user-dialed delays must drive load open-loop with latency
  from intended send time (coordinated omission), so the tool cannot distort the numbers it
  displays.
- **Doc-hygiene follow-up**: `parked-sidecar-embeds-web-gui.md` cites "KTD5 of the proxy plan" for
  its dependency freeze, but the plan's KTD5 is configuration-is-code — one-line citation fix.

Next step when picked up: choose an idea (21, the skateboard stack, is the fastest to something
running; 23, the bring-your-own-topic what-if machine, is the owner's own direction and the
strongest adoption artifact; 25, the live performance show, is the most fully specified) and take
it through brainstorm to a plan on `feats/proxy-requirements`.
