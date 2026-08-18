# astubbs/parallel-consumer#293 - state, and what the next session should pick up

Written 2026-08-18 at head `953cae87d`. **Delete this file when astubbs#293 merges**; it exists only
to hand the branch between sessions, and a stale handoff reads as live.

## Where it is

Branch `feats/proxy-requirements`, pushed and level with origin. Merged in this session: the
simplification passes, the shared-seam refactors, the foreign-client build work, the dependency
automation, the parallel session's ideation and KTD41, and `origin/master`.

**37 CI checks pass. Six do not**, and only two of the six are this branch's problem.

## The six reds, in the order they matter

**1. `clients: scala` - NEW, and the only unexplained one.** Failing step is `Conformance suite
(scala)`. The full 13x5 matrix passes locally at 65/65 on a 32-core box, and this row was green
before the fifth scenario landed. The job log ends in cleanup with `Terminate orphan process: pid
(8793) (java)` - **it hung rather than asserted**.

The hypothesis to test first, not a conclusion: the in-flight-ceiling scenario blocks on a cyclic
barrier of width = ceiling, plus a 250ms settle while the group is full. On a 2-core hosted runner a
client that cannot genuinely run two workers at once never fills the barrier and waits out the
budget. That is either **the scenario catching a real Scala client defect** - which is what it was
built for - or **the scenario being unsound on constrained hardware**, in which case it is unsound
for every language and got lucky elsewhere. Settle which before touching either.

**2. `Unit Tests` - a decision, not a defect.** The lane is *cancelled* at its 15-minute timeout, not
failing, and has been since 2026-08-15. It runs the conformance suite with no language selector, and
"absent means every registered binding", so it prebuilds all eleven runners - swift alone took
5m19s and was still going at the cut. The `clients:` matrix already runs the same suite per language,
on a runner that installed that toolchain.

Two levers: name the JVM set in `bin/ci-unit-test.sh`
(`-Dpc.conformance.language=core,java-direct,java-grpc,kotlin,scala`), or raise the timeout. **The
first is recommended and unmade** - it changes what a required check covers, which is the owner's
call, and the babysitting agent deliberately did not choose.

**3. `Chaos Pain Suite` - not this branch.** The open confluentinc#857 family; this branch's sighting
is recorded as the seventh in `bug-857-family.md` with replay seed `1870799285619636118`. Control
arm: sibling scenarios passed on their own seeds in the same run.

**4 and 5. `dups: clones` and `dups: similarity` - structural, and both predate this work.** Clones is
jscpd at +0.16% against a +0.1% delta, from the eleven per-language `pom.xml` blocks (the
`<lang>-e2e-harness` profile and the clean filesets). Similarity is the eleven `TestConventionsArchTest`
copies. The real fix for clones is hoisting the harness profile into the clients aggregator, which
changes activation for eleven modules - judged too risky mid-flight, and still is.

**6. `review: human LGTM` - the owner has not reviewed.** Correct state.

## The one inherited requirement nobody has checked

Master's astubbs#309 defers the native-core rewrite, and its safety argument rests on a requirement it
places **on this branch**: the conformance suite must pin engine behaviour at the **protocol
contract**, never at Java internals, so a second engine implementation can pass it black-box.

**Two concrete doubts, unaudited:** the `core` binding drives `ProxyHarness#start`, a plain
`ParallelEoSStreamProcessor`; and scenarios assert through `harness.awaitCommittedOffset(...)`, which
reads the mock consumer's commit history rather than anything on the wire. If a non-Java engine could
not pass the five scenarios as written, astubbs#309's framing is weaker than it reads. **An audit, not
a fix.**

## Known-shallow, deliberately

- **No client negotiates the `heartbeat` capability** - all eleven declare exactly `["dispatch"]`, so
  `leasesEnabled` is false and no lease can expire in the conformance lane. A liveness scenario would
  pass vacuously today; the engine side is ready and the `.proto` already carries `lease_duration` and
  `heartbeat_interval`. That is the U25 client wave, not a scenario addition, and the plan records a
  residual risk against R46 whose fix would change the scenario's shape.
- **No production entry point.** The only `main` is `TestModeMain` in `src/test`; the sidecar's
  lifecycle is plan unit U10, unlanded, mechanism settled by KTD19.
- **Nothing runs against a real broker** - the proxy has no `src/test-integration`.
- **Exactly-once is unreachable** through the proxy; `ExternalEngine` throws on transactional commit
  mode.

All four are stated in `docs/data/testing-evidence.d/parallel-consumer-proxy.yaml` rather than left
to be discovered.

## Stacked

astubbs/parallel-consumer#303 (`docs/module-readmes`) is stacked on this and retargets to master when
this merges.
