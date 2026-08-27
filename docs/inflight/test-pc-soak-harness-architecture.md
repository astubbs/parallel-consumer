# A PC soak harness: containerised worker nodes, a contention schedule, and a reaping story

<!-- inflight-type: feature -->
<!-- inflight-impact: blind-spot -->

Nobody runs this library for hours under sustained churn, because nobody here has a production
deployment to run it in. The defects this project keeps finding are the ones that need time and load.
A long soak against real containers is the nearest available substitute - and it is the only way to
exercise autoscaling behaviour at all, since that is a response to load over time and cannot be
observed in a minutes-long test.

## Shape

- **Docker to start**, so it builds and runs on a laptop before it needs a rig.
- **Worker nodes as constrained containers - one CPU, 4GB.** The constraint is the POINT, not a
  compromise. A 32-core host hides thread starvation, GC pauses interacting with poll timeouts, and
  the commit-response paths that only go wrong when the poll thread is genuinely starved. Several
  defects in the confluentinc#857 family are visible only under contention, and one signature has
  already been shown to replay red on a contended box and green on an uncontended one.
- **A chaos producer** feeding the topic, with its own load profile.
- **A chaos PC consumer whose USER FUNCTION latency follows a schedule** - reduced, stable, elevated
  - to simulate downstream systems slowing and recovering. This is the piece that makes
  astubbs/parallel-consumer#333's adaptive concurrency evaluable: without a controlled load
  trajectory there is nothing for an admission target to track, and nothing to say whether it tracked
  it well.
- **External scaling too**: spawn and retire worker containers on the same indicators, so group
  rebalances are driven by the workload rather than injected at random. That is a different rebalance
  shape from the current suite's, and closer to the redeployment scenario every field report
  describes.

## The reaping is the work, not the looping

A three-day run that fails once and buries the evidence costs three days and produces a rumour. Every
lesson this repo has already paid for points at capture rather than duration.

- **Split the logs by concern at the logback level**, so analysis is opening the right file rather
  than grepping a huge one. An autoscaling decision trace, a commit/offset trace and a
  rebalance/lifecycle trace are three different questions and should be three different files.
- **Emit the autoscaling track record as structured data, not prose.** What the admission target was,
  what drove the change, what the in-flight count and downstream latency were. A chart answers "did
  it track the load" in seconds; a log does not answer it at all.
- **Prefer time series over log lines for anything continuous.** PC already exposes metrics through
  micrometer; scraping them to a file makes the whole run a dataset. This is the single biggest
  difference between a soak that gets analysed and one that gets abandoned.
- **Ring-buffer the verbose stream and flush it only on a failure.** Three days of DEBUG is
  unusable and enormous - the chaos job log has already reached 126MB in minutes. Keep a rolling
  window in memory, write it out when something fires.
- **Capture the mechanism at the moment of failure, automatically**: a thread dump, plus PC's own
  internal counters. The six thread dumps that finally identified the revoke deadlock are the only
  reason it stopped being a signature and became a mechanism; every earlier sighting had the symptom
  and not the cause.
- **One line per iteration to a tally**, carrying the seed. Seeds outlive logs.

## Two things that will otherwise be misread

**A seed reproduces the SCHEDULE, not the outcome.** Real brokers, real containers and real timing
mean the same seed can produce different results - which is a finding to record, not a bug in the
harness. The seed makes the disturbance replayable; it does not make the system deterministic.

**Report what the run REACHED, not only whether it passed.** The recurring failure in this repo is a
green that never exercised the thing it claims to test: a mutation lane that scored nothing and
exited 0, a deadlock probe whose window never opened, both arms of an A/B going green with the
mechanism untouched. A soak with no notion of "did this run get to the interesting state" will
manufacture the same false confidence at much greater length.

## Where it can run

GitHub-hosted runners cap a job at six hours, so multi-day runs cannot live there. Self-hosted has no
GitHub-imposed job limit - the default `timeout-minutes` is 360 and can be raised - so the highcpu rig
can host a run of days, either through the runner or over SSH. Building it Docker-first keeps both
options open and keeps the laptop as the development loop.

## Do not build what already exists

Most of this is assembly, not invention. The bespoke part is small and this repo already owns it.

**Already in this repo, and the hard part.** `ProgressProbe`, `KeyOrderLedger`, `ChaosConductor` and
`ManagedPCInstance` are a working chaos conductor and correctness checker. They are calibrated,
which is the expensive bit - the note on a timing bound manufacturing its own evidence is what that
calibration cost. A containerised harness should DRIVE these, not replace them.

**Kafka's own Trogdor** is a fault-injection and long-running-soak framework built for exactly this
shape - agents, coordinator, workload specs, fault specs. Closest existing thing to the whole idea.
Worth an evaluation before writing an orchestrator.

**`kafka-verifiable-producer` / `kafka-verifiable-consumer`** ship with Kafka and do verifiable
produce/consume with sequence tracking - a ready-made external oracle for loss and duplication,
independent of PC's own accounting. An independent oracle is worth a lot here, because every
correctness claim currently comes from PC's own counters.

**Toxiproxy** gives the network faults this note admits are missing - latency, bandwidth limits,
partitions between PC and the broker - and has a Testcontainers module, so it fits the existing
harness rather than sitting beside it. This is the cheapest way to add a fault class nothing here
currently exercises.

**Testcontainers** is already a dependency and can orchestrate a multi-container topology including
compose, so the container story does not need new tooling either.

**Pumba** for container-level chaos (kill, pause, netem) if killing worker nodes is wanted - which is
the crash fidelity the suite explicitly cannot model today, since every stop it performs is an
orderly close.

**Prometheus and Grafana** for the time series. PC already exposes micrometer, so this is
configuration rather than code, and it turns "did the admission target track the load" into a chart.

**Logback already does the log routing**: `SiftingAppender` to split streams by logger or MDC, and
`CyclicBufferAppender` for the ring-buffer-flush-on-failure pattern. No new dependency, and
`logstash-logback-encoder` if the machine-readable stream should be JSON.

**Jepsen is the wrong shape** - Clojure, and aimed at linearizability of stores - but its structure is
worth stealing outright: generator, nemesis, checker. That is precisely conductor, chaos schedule and
correctness ledger, and this repo has all three already.

**So the genuinely new work is narrow**: the downstream-latency schedule that drives autoscaling, the
external scaler, the container topology, and the reaping described above.

## What this still is not

Not production. No network partitions, no broker upgrades or failovers, no competing workloads from
other applications, no multi-AZ latency. Worth stating so the results are not overclaimed: it
exercises PC against load and churn, which is more than anything here does today, and less than a
deployment.
