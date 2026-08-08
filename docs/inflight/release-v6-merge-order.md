# v0.6.0.0 merge order - what goes in, what waits, and why

Ordering decision as of 2026-08-08. The release itself is #197; mechanics live in
[`release-0.6.0.0.md`](release-0.6.0.0.md). This file answers the one thing no command can: **given
everything in flight, what does v6 contain and in what order** - so it is not re-derived from scratch
every session.

`gh pr list` is the authority on what is open, and titles/state are deliberately not copied here.
Coverage was checked by diffing the numbering below against that list; re-check with:

```bash
gh pr list -R astubbs/parallel-consumer --state open --limit 80 --json number --jq '.[].number' | sort -n
```

**Ordering is by value to the codebase and to the release, not by merge mechanics.** Conflicts,
red checks and stale bases are not inputs - they are Monday's work, and an LLM resolves them cheaply.
Where an item genuinely *must* precede another, it says so.

## Before the tag

**1. Transactional atomicity.** The fork's debut release cannot ship a known silent violation of the
one guarantee transactional mode exists to provide.

1. **#261** - a terminally failed send leaves a partial result set visible to `read_committed`, and
   the instance neither fails nor shuts down
2. **#257** - transactional batches redeliver records that succeeded, and blame the user function
3. **#262** - proves or falsifies all fourteen documented transactional guarantees. *Depends on 1+2.*
   This is the evidence for them; without it the release asserts correctness rather than showing it.

**2. Loss, exhaustion, and the #857 family.**

4. **control-loop hook CME** - no PR yet; handed off on `feats/web-gui` in `a7e796a5`, see
   [`bug-control-loop-hooks-cme.md`](bug-control-loop-hooks-cme.md) *on that branch*. Extract first:
   every other in-flight branch carries the defect until it is on `master`.
5. **#31** - a record is permanently lost until pod restart after rebalance (upstream #909)
6. **#29** - the last open #857-family defect. Shipping two thirds of that family is the odd outcome,
   since the changelog already advertises its siblings. See [`bug-857-family.md`](bug-857-family.md).
7. **#57** - unbounded PCMetrics heap growth (upstream #859)
8. **#207** - an unknown magic byte kills an older reader regardless of `invalidOffsetMetadataPolicy`.
   **The one item with a real deadline**: it decides what v6 readers *already deployed* will tolerate
   from future writers, and no later release can retrofit tolerance into them.

**3. What v6 demonstrates that upstream never did.** New surface is deliberate here: for a first
release whose job is to make "actively maintained" credible, shipping the surface is the point.

9. **#226** - health-check on the interface (#126, open since 2021)
10. **#205** - MDC propagation into the worker pool and the vert.x event loop
11. **#202** - `LongPollingMockConsumer` into the main artefact (#159). Packaging shape belongs at an
    `x.y.0` boundary; adding to the published jar in a patch is the worse version of the decision.
12. **#116** - the JStream disposition (#122). Two contradictory directions are in flight - #116
    deprecates as won't-fix, `feats/jstream-bounded-blocking-buffer` bounds the buffer instead, and
    the latter is *not* a descendant of #116's head. Antony is resolving which one ships.

**4. What an operator hits on day one.**

13. **#203** - bound the dropped-batch and user-function-failure logs (#169, #170)
14. **#201** - the load-factor WARN fires every control-loop pass for anyone following the README's
    own tuning advice (#155)
15. **#204** - report the poll thread's real error, not the commit-response timeout (#177)
16. **#200** - why `ManagedTruth` "cannot be found" (#180); the first wall a new contributor hits

**5. Release machinery - worthless if it lands after the tag.**

17. **#259** - pin `central-publishing`. v6 *is* a Central publish; an unpinned publishing plugin is
    the wrong thing to discover during one.
18. **#199** - publish the curated changelog as the Release body. **The only item that cannot be
    applied retroactively**: land it after the tag and v6's release page stays blank permanently.

**6. Make "green" mean something before tagging.**

19. **#224** - stop retrying failing tests; all three CI entry points were swallowing
    `rerunFailingTestsCount=2` and nothing read the `Flakes:` lines
20. **`test/suite-config-isolation-and-event-based-waits`** - pushed, no PR. The core suite was
    configuring *other* modules through `junit-platform.properties` and asserting on clocks.
21. **#260** - tolerate PC's repeat commit of the same base offset
22. **#263** - audit every test that does not run, assert, or exist → 23. **#264** acts on it
24. **#206** - one MockConsumer harness (#40)

**7. New opt-in modules.** Each is a separate module, experimental, and off unless asked for; core
coupling is a root-pom line. Sized by diff they look like release risk, and they are not - checked.

25. **web GUI (#215)** - `feats/web-gui`. The best single artefact for the "maintained, and past where
    upstream stopped" claim, and v6 is the release that claim has to land in.
26. **KS PoC (#255)** - `feats/ks-on-pc-spike`, with plan docs on
    `docs/assess-kafka-streams-pc-integration`. **Publishing status is a live decision** - the root
    pom comment currently says "never published" and that is being changed to published-if-it-works.
27. **Connect PoC (#240)** - `feats/connect-on-pc-spike`. **The only v6 item with schedule risk**: the
    branch is plan-and-docs today and the PoC itself is not written yet. Everything else already exists.

**8. Adoption surface and zero-risk docs.**

28. **#223** - STRATEGY.md and the KIP-932 Share Groups comparison; the document that answers "why
    this fork" for everyone arriving from the release announcement
29. **`feats/industry-grounded-examples`** - **local-only, unpushed.** Rewrites the core/reactor/vertx
    examples as real use cases. The examples are what a first-time reader actually runs.
30. **`feats/streams-state-store-enrichment-example`** - **local-only, unpushed.** Reads a Streams
    state store from PC, with a load test reporting what the concurrency bought. Pairs with 26.
31. **#258** - the 2023 upstream administrative sweep
32. **#256** - issue auto-answer, shipped switched off

## → Cut v0.6.0.0

## After the tag

33. **#51** virtual threads · 34. **#106** sparse offset encoding, *after* #207 settles the encoder ·
35. **#53** Java baseline + Kafka 4 (0.7) · 36. **#105** unit-gate packing · 37. **#1** CodeQL, to
reconcile against default setup, which already covers `actions`/`java-kotlin`/`python` · 38. **#38**
JUnit 6, blocked on ArchUnit having no JUnit 6 engine · 39. **#8** DLQ, as its own project

## Open decisions this order does not settle

- **Which modules v6 publishes.** `parallel-consumer-dashboard` has no `skipPublishing`, so as it
  stands v6 publishes it to Central as a coordinate supported from then on. The KS spike's status is
  changing in the same direction. Experimental-and-published is defensible; it should be chosen.
- **#197's body is stale.** Both its "Blocking" checkboxes were closed by #198, and #171/#194 shipped.
  It is the handle everyone links to, and it reads as more blocked than it is.
- **Two branches exist only in local worktrees** (29, 30 above) - invisible to everyone else, and lost
  with the machine. Push them whether or not the PRs open.
