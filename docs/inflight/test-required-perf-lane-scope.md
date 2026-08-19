# What belongs in the required Performance Tests lane

<!-- inflight-class: blind-spot -->


`bin/performance-test.sh` is the "Performance Tests" leg of `maven.yml` - **a required check on every PR**,
`ubuntu-latest`, `timeout-minutes: 60`, and no retry (`AGENTS.md`: a flake fails the build, deliberately).
Anything `@Tag("performance")` therefore blocks merges. Two questions about that scope are open.

## Decided: the 40,000-message rung stays automatic

`LoadTest.asyncConsumeAndProcessAtVolume` runs there at `HIGH_VOLUME_TOTAL` (40,000), ten times the gating
volume, on a class listed at **1/20, undiagnosed** in the load-tightness flake family
([`test-load-tightness-flakes.md`](test-load-tightness-flakes.md)) at its *gating* volume.

Measured before leaving it automatic, per the `AGENTS.md` rule to separate contention from a real
concurrency bug: on an uncontended broker it ran **5/5 green at ~52s against a derived 600s ceiling**. So the
code is fine at this volume and nowhere near its deadline. It then passed the real CI lane on first run.
Left automatic on that evidence.

## Open: the lane pulls in a Postgres container nothing there uses

`LoadTest extends DbTest`, and `DbTest` starts a `PostgreSQLContainer` in a **static initialiser**. Tagging
one method `performance` is enough to class-load it, so from astubbs#264 onward the required lane starts and
holds a database that no test in it reads or writes - `asyncConsumeAndProcess`'s only DB call is commented
out. `DbTest` is itself listed at **2/20** in the same flake family, for "postgres container start under
contention".

So the lane inherited a known-flaky container start, and its memory, to run a test that never touches a
database. Options:

1. **Move the volume case to a class extending `BrokerIntegrationTest`** rather than `DbTest`, leaving the
   DB-shaped tests behind. Cleanest; no test in `LoadTest` appears to need Postgres.
2. **Make `DbTest`'s container lazy** (behind an accessor rather than a static initialiser) so class loading
   alone does not pay for it. Helps every DB-shaped test, not just this one.
3. **Leave it** and watch, as with the volume decision.

Related, and worth deciding together: the lane declares no `forkCount`, so all four `@Tag("performance")`
classes run in **one fork**, sharing one heap and the static broker. They run *sequentially* - `-Pci` sets
`parallel-tests=false`, and failsafe's `<parallel>` element is not read by the JUnit Platform provider at
all - so this is a heap-and-wall-clock question, not a concurrency one. The gating integration lane already
forks (`-DforkCount=4 -DreuseForks=true`); this lane never got that treatment.

## Watch: `-Dload.total` moves the gating tests too

`LoadTest.total` and `asyncConsumeAndProcessAtVolume` read the **same** property. Reaching the documented top
rung with `-Dload.total=400000` also raises the two untagged gating tests to 400,000, each with a derived
100-minute ceiling against a 60-minute job cap. Fine when run deliberately outside CI; a trap if anyone puts
that flag in a lane.
