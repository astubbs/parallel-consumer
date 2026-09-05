# Demos - measurements for eyes

This doc **owns how this project does demos**: what a demo is, the gates and conventions that bind
one, and the inventory. The root `AGENTS.md` routes here. The web GUI demo track is separate
(`docs/inflight/` `web-` notes); this doc covers the terminal demos that live in the repo and run
against a real broker.

**A demo is a measurement whose reader is a person.** It answers "show me", not "is it correct" -
so it carries no assertions, runs real infrastructure, and optimises its output for eyes. The
worked example that set the current bar is `NavigatorDemo` (`bin/demo-navigator.sh`): a banner
saying what is about to happen, a per-second dashboard while it happens, and a plain-words
"WHAT JUST HAPPENED" summary - a ~30 second storyline a stranger can follow without knowing the
codebase.

## The rules

- **Every demo has an asserted twin, and the demo is never the evidence.** The same storyline must
  exist as a CI-gated test before it exists as a demo - `NavigatorDemo` is the eyes-optimised twin
  of `NavigatorPartitionShareIT`; `AdaptiveConcurrencyDemo` demonstrates what the adaptive suites
  assert. A demo with no asserted twin is a claim nobody checks, and it will drift into fiction the
  first time the engine changes. The twin also frees the demo from measurement scaffolding: the
  demo shows, the test proves.

- **Off by default.** A no-assertion multi-second measurement must not tax every build: demos live
  in `src/test-integration` (failsafe-collected), gated behind
  `@EnabledIfSystemProperty(named = "pc.demo", matches = "true")`. The classic vertx `Demo`
  established the flag; every demo since reuses it.

- **One command to run it: `bin/demo-<name>.sh`.** A wrapper that preflights its dependencies with
  a friendly failure (Docker down = a two-line explanation, not a TestContainers stack trace), runs
  the failsafe route quietly (`mvnw -q`), and passes extra arguments through. The class javadoc
  carries the raw `mvnw` invocation too, for environments that cannot run the script. Never give a
  demo script a `check-` or `test-check-` prefix - those names grant reviewer execution by pattern
  ([`bin/AGENTS.md`](../bin/AGENTS.md) owns that rule).

- **The dashboard is the show: stdout only, never a logger in the story loop.** Log-line prefixes
  (timestamp, instance tag, thread, source location) are what make an integration test's output
  unreadable as a demo - that observation is what created this doc. The demo prints its narrative
  with `System.out`; anything the harness logs at INFO over the storyline gets its logger lowered
  **in the demo's fork only** - and note `logback-test.xml` pins some harness diagnostics loggers
  explicitly, so the exact pinned logger must be re-levelled (a parent-level set does not override
  an explicit child pin), from a static initialiser when the base class touches clients in
  `@BeforeEach`. `NavigatorDemo`'s `quietLogsForTheAudience` is the pattern. The asserted twin
  never adjusts levels except to capture what it asserts.

- **Show the contrast, not just the feature.** A number without a "without it" number demonstrates
  nothing. Every demo carries an untouched arm: `NavigatorDemo`'s untagged bystander drains
  flat-out beside the throttled instances; `AdaptiveConcurrencyDemo` runs the library default and
  the hand-tuned guess against the adaptive arm. The contrast arm is also what makes the demo
  honest - it proves the effect is the feature's, not the environment's.

- **Nobody tunes the answer into the demo.** The demo's downstream carries its own truth (capacity,
  rate, knee) and the feature has to discover or enforce it on camera -
  `AdaptiveConcurrencyDemo`'s downstreams are the worked example ("capacity NOBODY CONFIGURES INTO
  PC"). A demo whose knobs are set to produce its conclusion is an advertisement, and reviewers
  should treat it as one.

- **Simulated work is a semaphore around a sleep** (the simulated-work rule - this doc is its home;
  `AdaptiveConcurrencyDemo` cites it). A sleep holds a worker without burning CPU, and the
  semaphore is what gives the simulated downstream a real capacity for the feature to find. When
  the story is genuinely about CPU, use real CPU work and say so - and calibrate it in the run's
  own output, the way the CPU plant logs its serial baseline first.

- **Real infrastructure.** Demos claim reality, so they run a real broker (TestContainers), real
  consumer groups, real rebalances. A demo against mocks demonstrates the mocks.

- **Verify it with your own eyes before shipping - at least twice.** Run it, read every line as a
  stranger would, fix what reads wrong, run it again. A demo is UI; the repo's verify-UI rule
  applies with no discount. Wall-clock storylines also vary run to run - two clean runs is the
  floor, not paranoia.

- **Keep the storyline under about a minute** once infrastructure is up. Longer measurement belongs
  to the perf suites; a demo that outlasts its audience's attention has failed at its one job.

- **A demo is user-visible: its commit carries a `Release-Note:` trailer**, and the PR body's
  description names the run command ([`docs/inflight/AGENTS.md`](inflight/AGENTS.md) owns the
  trailer mechanics).

## The demos that exist

| Demo | Asserted twin | The story | Run |
|---|---|---|---|
| `NavigatorDemo` (core) | `NavigatorPartitionShareIT` (AE1, AE2) | Two separate JVMs in one consumer group split one 2/s resource at ~1Hz each, holding one partition of two apiece and each reporting share 0.500; a tagless bystander process drains flat-out; one tagged JVM is KILLED and, once the missed heartbeats add up, the survivor inherits the whole rate at share 1.000; the fleet's books are collected from the broker and printed | `bin/demo-navigator.sh` |
| `AdaptiveConcurrencyDemo` (core) | the adaptive-concurrency suites | Adaptive discovery finds downstream capacity nobody configured, against the default and an over-provisioned guess | javadoc route (`-Dit.test=AdaptiveConcurrencyDemo`) |
| `Demo` (vertx, the classic) | the vertx suites | The 2021 cast's shape: how fast PC eats a backlog when every record needs an HTTP call | javadoc route (`-Dit.test=Demo -Dpc.demo=true`) |

When you add a demo, add its row - and give it a `bin/demo-*.sh` wrapper even though the two older
demos predate that rule; retrofit wrappers when a demo is next touched.
