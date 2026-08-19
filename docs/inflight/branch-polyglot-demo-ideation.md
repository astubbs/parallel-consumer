# Polyglot demo app — ideation done, direction not yet chosen

> Extracted from `origin/docs/ideate-polyglot-demo` @205ddacc1, `docs/inflight/branch-polyglot-demo-ideation.md`.

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


## Owner addition 2026-08-19: the run-mode experiment harness, which is direction 3's engine

Antony, after the confluentinc#857 chaos measurements produced a four-cell assignor x stop-mode
matrix with live per-poll output: **the demo app should run those experiments and display their
results in real time.** It lands on direction 3 - the bring-your-own-topic what-if machine - not as
a new direction but as the thing that makes it concrete, because "what if" needs an experiment to
run and a result to show.

What the chaos work already built that this would reuse:

- **The run modes are the demo's variables**: assignor (eager vs `CooperativeStickyAssignor`),
  close mode (`close()` vs `closeDrainFirst()`), commit mode, ordering mode. The matrix run on
  2026-08-19 shows the payoff - duplicates 2,421 / 2,007 / 405 / 369 across four cells of the same
  workload, which is the kind of difference a user cannot get from prose.
- **The chaos harness is toggleable, and should be a switch in the UI, not a fork of the app.**
  Chaos ON is the dramatic demo; chaos OFF against the user's own topic is the honest one, and it is
  the same harness either way. `ChaosConductor` is already seed-driven and replayable, so a shown
  result carries a command that reproduces it.
- **Live output already exists in the right shape**: the diagnostic mode emits
  `consumed / started / inFlight / violations` per poll. That is a real-time series a UI can render
  directly, and `inFlight` is what makes a flat completion count legible - the demo would show
  *work in progress*, not just a throughput number, which is the whole point of the library.

Two design points from the owner:

- **Parallel or sequential runs, both with live display.** Sequential is the fair comparison, since
  concurrent cells contend for CPU and broker - a confound already measured in the chaos work.
  Parallel is faster and better television. The choice must be explicit and its effect on the
  numbers stated in the UI, not left for the viewer to infer.
- **Pointing it at a real topic is the differentiator.** A benchmark someone runs on their own
  cluster, with their own record sizes and their own processing function, answers a question a
  published table never can.

Boundary against the perf-comparison track, applying the note above: this is the **app** running
experiments interactively and showing them live. Blessed numbers, workload definitions and
measurement semantics stay the perf track's. A number shown here is illustrative and must say so -
the chaos suite's duplicate rates are deliberately hostile and are already captioned that way in the
README's `reducing-duplicate-replay` section.
