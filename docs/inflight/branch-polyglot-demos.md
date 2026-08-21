# The ten per-language demos: ten branches, no PR, and what the fan-out found

Session handoff, 2026-08-21. Ten agents built the per-language demos in parallel, one worktree and
one branch each. **Nothing is merged and only `feats/java-sidecar-demo` is pushed.** This file exists
because the work is real, on disk, and invisible to `gh`.

## Where the code is

Branched from `feats/polyglot-demos` at **`e0d3a95bf`**, which is itself stacked on
`feats/java-sidecar-demo` (astubbs#328, which is stacked on astubbs#293).

| branch | worktree under `.claude/worktrees/` | commits |
|---|---|---|
| `demos/python` | `demo-python` | 2 |
| `demos/typescript` | `demo-typescript` | 4 |
| `demos/go` | `demo-go` | 2 |
| `demos/rust` | `demo-rust` | 4 |
| `demos/kotlin` | `demo-kotlin` | 2 |
| `demos/scala` | `demo-scala` | 2 |
| `demos/swift` | `demo-swift` | 1 |
| `demos/cpp` | `demo-cpp` | 2 |
| `demos/dotnet` | `demo-dotnet` | 2 |
| `demos/ruby` | `demo-ruby` | 2 |

Each demo lives in its own client module's `demo/` directory. Per-language decisions and divergences
are in `docs/inflight/clients/<lang>.md` on each branch - **read those before touching a demo**, they
carry the reasoning the code cannot.

**No agent added a Maven module**, deliberately: that needs an aggregator pom edit, which is the one
genuinely shared file and would have made ten disjoint branches collide. Kotlin and Scala use opt-in
profiles instead; Kotlin verified with a reactor listing that its module still has no edge to
`parallel-consumer-proxy`, and its note says Scala should copy that arrangement.

## The branches predate two fixes to the thing they copied

`feats/polyglot-demos` is at `e0d3a95bf`; the seed has since moved to `e95b9a0fb`. Two of those later
commits fix defects **in the compose file and Dockerfile the ten demos were transcribed from**:

- every `PC_DEMO_*` dial except `BOOTSTRAP` and `ARGS` was silently dropped on the container path;
- `docker compose run demo --help` never reached the parser.

**Ruby found both and fixed them in its own demo. The other nine probably inherited them.** Check
each language's `docker-compose.yml` for a full `environment:` block and its entry point for a
trailing `"$@"` before believing its environment precedence works. Details in the commit message of
`e95b9a0fb`.

## The contract is wrong in ways only ten implementers could show

`parallel-consumer-proxy/demo/README.md` is the shared contract. **No agent edited it** - they
recorded objections in their own notes, by instruction. These are unresolved:

1. **The per-language wait rule keys on the wrong thing.** The contract lists nine languages where a
   blocking sleep is fine. **Rust, Kotlin, Swift and C# are all on that list and all four found it
   wrong for their client.** Kotlin named the correct predicate: *is the client thread-per-record?*
   Ruby, asked to check rather than assume, confirmed the rule IS right for Ruby - MRI releases the
   GVL around `sleep` and its executors are threads. That negative result is what makes the fix
   certain. Rust ran the control: 10,341 msg/s through the library's blocking adapter versus 3,518
   with a raw thread sleep, a prediction it stated before running.
2. **Who starts the broker was never stated**, so Java, Python, Go and TypeScript each answered
   differently. Owner decision: the entry point brings up the compose broker; the demo program only
   ever connects. **For testing, one shared broker and one reused topic, never torn down** - which
   also removes the need for any client to create a topic, and so removes Swift's blocker (its Kafka
   client has no admin API and cannot create or verify partitions at all).
3. **`--partitions` should configure the broker the entry point starts**, not be a `CreateTopics`
   call eleven libraries implement differently, and it is inert against a supplied `--bootstrap`.
4. **The sidecar-location variable has three names already** (`PC_DEMO_SIDECAR`,
   `PC_DEMO_SIDECAR_CLASSPATH`, `PC_DEMO_SIDECAR_JAVA`). The contract's flag table has no slot for it
   and every non-JVM demo needs one.
5. **The credential rule binds only the fingerprint block**, while the Kafka clients dump
   `bootstrap.servers` at INFO several times a run. It should bind the whole run.
6. **`bin/ci-demo-test.sh` is Java-only** - confirmed independently by every agent. Ten demos can
   ship while the contract claims both entry points are tested per language.
7. The big-replay title's `total * delayMs / 1000` is integer arithmetic and prints "would take 0s+"
   at small volumes. A seed wart, faithfully mirrored everywhere.
8. **Every non-JVM demo container is a two-toolchain image** and the contract does not say so - the
   sidecar is a child of the *running* demo, so it cannot be a discarded build stage.

## Product defects the fan-out found

- [`bug-sidecar-runtime-logging-and-address-leak.md`](bug-sidecar-runtime-logging-and-address-leak.md)
  - one defect with five faces, found by five agents, where **each one's proposed fix leaves another
  standing**. Includes the fix ordering and why step 0 (move the sidecar's logging off stdout) is not
  a guess: Rust showed stdout is the lifecycle channel and is discarded, C++ showed its client
  inherits stderr deliberately and demonstrated the chain working.
- [`bug-direct-client-does-not-disable-auto-commit.md`](bug-direct-client-does-not-disable-auto-commit.md)
  - unchanged from earlier in the session; still needs an owner decision.
- **.NET, outside its demo directory and in its own revertible commit:** `Grpc.Tools` 2.71.0's
  bundled arm64 protoc segfaults under MSBuild in a container. Established with a control - one
  variable changed, reproduced by hand at both versions, and building fine under
  `--platform linux/amd64`, which is why CI's amd64 runners never saw it. Bumped to 2.83.0.
- **Rust, in its own module:** `build.rs` preferred a `protoc` it could not execute (the `~/.m2`
  fallback is consulted before `PATH` and selected on `is_file()` alone; Maven stores it `0644`).
  Its defect-class search found Go immune only because Go `chmod +x`es the user's Maven repo.

## What was NOT measured, and why that matters

**Almost nothing.** Load average ran 20-83 on 12 cores throughout. Every agent was instructed to
prove the machinery and refuse to report throughput, and every one complied. Treat any figure in any
of these branches as "it ran", never as a measurement.

Specifically unrun: **Scala's container path** (machine saturated), **Ruby natively** (this box has
Ruby 2.6.10, the floor is 3.2), and **Swift and C++ have no native mode at all** by design - their
toolchains do not exist on a dev box here, so the container *is* the toolchain.

The deferred idle-machine measurement pass is therefore load-bearing, not polish. It is also blocked
by an open question of its own: the AK core baseline moved 344-346 to 299-303 msg/s between sessions
on the same machine, a control arm refuted the obvious cause, and it is the denominator of every
ratio. See [`next-demo-seed-followups.md`](next-demo-seed-followups.md) item 1.

## Wrong paths - do not retry

- **Do not add a Maven module for a demo.** It forces an aggregator pom edit; that is why none of the
  ten did.
- **Do not "fix" the sidecar logging by moving to `-DincludeScope=runtime` alone.** Two agents
  recommended it; it drops the test jar that is currently the *only* thing suppressing the
  `bootstrap.servers` dump. The ordering in the bug file exists for this reason.
- **Do not serialise the fan-out on Python.** The reasoning that Python must go first - that it might
  falsify the contract - was wrong: the contract already carves Python out by name, so it cannot.
  Ten parallel worktrees worked.
- **Do not read "kotlin failed" or "rust failed" in CI as a client defect.** Every language job runs
  the whole conformance suite including the in-process `core` binding, so the failing arm is often
  core's. See [`test-untracked-ci-flakes.md`](test-untracked-ci-flakes.md).

## Not started

U35's second half - the *reading* demo (three modes, TTY prompt, serde marker, sampled output) - and
the reconciliation it needs: **KTD40 in the plan still describes every demo as having three modes**,
while the seed has none and says why. Plan and seed disagree on paper until someone amends KTD40 or
records that it governs the reading demo only. That reconciliation, not the code, is the open item.


## What is next, in order (2026-08-21)

1. **The conformance harness must land with the polish wave, not after it.** The arm labels now carry
   the client name in parentheses and the table gained two columns; both break the old parser. Two
   language agents ran `skeleton()` over their own output and **measured** the dangerous outcome: the
   fingerprint still parsed, so the skeleton was non-empty, the skip never fired, the absolute
   assertions passed, and the drift check silently narrowed to comparing two headings. **A green run
   would have meant the tables were not checked at all.** Fixed on `feats/polyglot-demos` - the parser
   now tolerates parenthesised labels, compares the deterministic `records`/`keys` **values** instead
   of masking them, and a demo whose table cannot be parsed **fails** rather than skipping.
2. **Merge the eleven `polish/*` branches**, normalise any column order that differs from the
   contract's now-fixed one, then run the harness across as many languages as the machine allows.
   This is the first point at which the drift check proves anything beyond two languages.
3. **Push once**, then decide the PR shape. Current recommendation is to fold
   `feats/polyglot-demos` into astubbs#328 rather than open a second PR, because that PR still ships the
   **old, disproven** divergence rule and the correction's evidence is the fan-out itself - reviewing
   one without the other reviews a conclusion with no argument. Reverse that if astubbs#293 merges
   while the polish wave is still unfinished.
4. **The idle-machine measurement pass**, which is blocked twice: it needs a quiet box, and it needs
   the unexplained baseline shift in [`next-demo-seed-followups.md`](next-demo-seed-followups.md)
   item 1 resolved, because every ratio divides by a figure that moved 15% between sessions for
   reasons a control arm refuted.
5. **The sidecar logging defect**, in the order its own record specifies - see
   [`bug-sidecar-runtime-logging-and-address-leak.md`](bug-sidecar-runtime-logging-and-address-leak.md).
   Each step alone regresses something, which is why it is one job.
6. **Owner decisions, not work**: the `enable.auto.commit` divergence between the two transports; the
   KTD40 reconciliation (the plan still says every demo has three modes, the seed has none and says
   why); and whether the merged `demos/*` branches and their worktrees are cleaned up.

### A fix that was applied and NOT verified - treat as unproven

`KAFKA_LOG4J_ROOT_LOGLEVEL: WARN` was added to all eleven compose files to quieten the broker, and
**it does not work**. Two language agents measured it independently - 927 broker lines against 35 of
the demo's own in one run, 584 in another - and root-caused it: the cp-kafka image writes its own
per-logger map (`kafka=INFO`, `kafka.controller=TRACE`, `state.change.logger=TRACE`) which outranks
the root level. `KAFKA_LOG4J_LOGGERS` is the variable that merges into that map and has now been
added to all eleven, **but the verifying run was cut off by a timeout and has not been repeated.**
Count the broker lines before believing it.

Nothing counts broker lines today, and the native path never sees them because Testcontainers does
not attach the broker's stdout - so this was invisible to every check that exists. A line-count
assertion in the harness would close that.


## Reconciliation items found during the polish wave

- **Two languages placed the new columns differently, and both were reasoning correctly.** Rust and
  .NET put `records | keys` *after* `vs AK core`; python, typescript, scala, kotlin and go put them
  immediately after `arm`. Both Rust and .NET said explicitly why: the harness's `HEADER` pattern was
  **not end-anchored**, so appending was the only position that kept the check passing, while its
  `ROW` pattern was end-anchored and broke either way.

  **That is the lesson worth keeping.** The contract was silent on position and the regex was not, so
  two independent implementers let a broken test define the spec - which is what an under-specified
  contract plus a visible check will always produce. The contract now fixes the order and the parser
  is repaired, so this is one format string each at merge. **Check all eleven**: the drift check was
  structurally unable to tell them apart while its row pattern matched nothing.

  Final tally across the nine that landed: **five beside `arm`** (python, typescript, scala, kotlin,
  go), **three appended after `vs AK core`** (rust, dotnet, java) and **one in the middle** (cpp, at
  `arm | elapsed | msg/s | records | keys | vs AK core`). Three different answers from nine
  implementers reading the same document - which is what an unstated rule costs, and why the contract
  now states it.

- **Ruby and Rust both publish host port 29092**, so their demos cannot run back to back without
  `Bind for 0.0.0.0:29092 failed`. Python already parameterises its port
  (`${PC_DEMO_BROKER_PORT:-19095}`), which is the shape that fixes it. Patching one language alone
  only moves the collision.

  **This is also an argument for the shared-broker test harness** already agreed: one broker and one
  reused topic for the whole suite removes per-language ports entirely, rather than allocating eleven
  non-colliding ones.

- **Eleven concurrent demos exhaust the Docker VM's disk**, and it does not present as a disk
  problem. Three agents hit it within minutes of each other; the broker exits 1 having logged
  `Formatting metadata directory ... No space left on device`, and the demo reports only "starting
  the broker failed". Worth a friendlier message, and worth knowing before running the full eleven
  again on one machine.


## A third wave is owed: every serious client, as its own arm

Owner correction (2026-08-21): **excluding sarama from the Go demo was wrong, and so is the
equivalent exclusion in every other language.** The contract said "consider running both"; it now
says run them all. Each of these is somebody's production choice, and answering "is this fast in my
language" for one client out of three leaves two thirds of readers guessing.

Known to be missing, from the agents' own READMEs:

| language | runs | also serious, not yet an arm |
|---|---|---|
| Go | franz-go | **sarama**; confluent-kafka-go (blocked by `CGO_ENABLED=0` in its image - a real constraint, and the kind worth writing down) |
| Python | confluent-kafka | kafka-python, aiokafka |
| TypeScript | kafkajs | confluentinc-kafka-javascript (librdkafka binding) |
| Ruby | rdkafka | ruby-kafka is archived - a legitimate exclusion, and it belongs in the README with its reason |
| others | one each | unaudited; each demo's README must now name what it found and what it skipped, with reasons |

Two things had to change before this was even possible, and both are done: the harness compares the
**required** arms and permits extras, and Java is no longer exempt from that comparison. Go had
already costed a sarama arm, found it cheap, and declined **because the harness would have failed it
for obeying the contract** - so the rule and the check were in direct conflict.

## And the internals are NOT the seed's to dictate

Owner correction, same conversation: **the Java demo is a seed, not a template.** Uniformity is owed
on everything the contract specifies - flags, banner, tables, arms, container rules, exit codes - and
on nothing below that line. Each demo should be written the way someone fluent in that language would
write it: its idioms, its concurrency primitives, its layout, its test framework.

This is now stated in the contract under "Idiomatic inside, identical outside", because the risk was
live rather than theoretical: eleven agents were handed a Java reference and told to mirror a
contract, and the natural failure mode is transliteration. A Java program rewritten in Ruby teaches a
Ruby reader nothing about using this client in Ruby.
