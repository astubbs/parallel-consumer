# Next: franz-go as a client for the non-JVM proxies - what it buys and what it costs

<!-- inflight-type: task -->
<!-- inflight-impact: architecture -->

Opened 2026-08-21. **Prior art checked**: no existing inflight entry covers Go client selection; the
language-proxy fan-out (astubbs#242, `clients/*.md`) assumes a proxy to the JVM engine and does not
discuss which native client a proxy would use. This is the first note on the question.

## Why it is being asked now

A controlled benchmark, not a hunch. `bench/franz` drives **franz-go with no engine at all** and
`bench/` now has a matching **Java floor** (`pool`: plain `KafkaConsumer`, thread pool, semaphore,
sleep). Both floors, same broker, same dataset, all keys distinct
([`bench/results/high-concurrency-unordered.csv`](../../bench/results/high-concurrency-unordered.csv)):

| Delay / concurrency | Java floor | Go floor | Java as % of Go |
|---|---:|---:|---:|
| 0ms / 250 | 97,201 | 144,009 | **67%** |
| 2ms / 5,000 | 43,482 | 138,427 | **31%** |
| 100ms / 5,000 | 20,102 | 37,600 | **53%** |

**No engine on either side.** That gap is `kafka-clients` versus franz-go, and it is larger than
anything Parallel Consumer's engine contributes - PC sits within a few percent of the Java floor at
most points and beats it at two. See
[`bug-in-flight-ceiling-above-2000-concurrency.md`](bug-in-flight-ceiling-above-2000-concurrency.md).

**The Go floor also reaches a concurrency of 5,000 exactly, where both Java arms plateau near 2,800.**

## CORRECTION 2026-08-22: virtual threads close most of this gap. The measurement is done.

**The table above is superseded.** It was taken on 2026-08-21, before virtual threads, and the note
already suspected the in-flight column was the tell. It was. The control has now been run: **the same
Java floor, one term changed - workers are virtual threads instead of platform threads.**

200,000 records, ten partitions, `BENCH_TIMER_CALLEE=1` so no HTTP stub or socket is in the path,
concurrency 5,000, two repeats, load 7-33:

| Arm | 0ms | as % of the Go floor | 2ms |
|---|---:|---:|---:|
| **`franz`** - the Go floor | **62,384** | - | serial, skipped |
| **`pool-vt`** - the Java floor, virtual threads | **58,064** | **93%** | **55,890** |
| `llingr` | 56,681 | 91% | 56,698 |
| `core-dpvt` | 50,768 | 81% | 49,653 |
| `core-vt` | 49,365 | 79% | 50,283 |
| **`pool`** - the Java floor, platform threads | 42,803 | **69%** | 25,957 |
| `core` - shipped engine | 40,045 | 64% | 27,602 |

**The Java client reaches 93% of franz-go once its workers are virtual threads, against 69% on
platform threads.** At 2ms the same change is worth **2.15x** - 55,890 against 25,957, same client,
same broker, same records, only the thread model different.

**So the 31-67% figure was mostly measuring platform threads, not `kafka-clients`.** The
remaining client gap is about **7%** at 0ms, and it is the honest size of what a Go client would buy.

**Two further readings worth keeping:**

- **`pool-vt` beats llingr at 0ms** - 58,064 against 56,681. A bare Java consumer with a
  virtual-thread pool is faster than llingr's entire engine, on this workload.
- **PC costs about 13% over the bare Java floor** - `core-dpvt` 50,768 against `pool-vt` 58,064, and
  11% at 2ms. That is the per-record machinery, and it is the quantity
  [`core-auto-scaling.md`](core-auto-scaling.md)'s inline-execution idea would attack.

**The old table is left above deliberately, not deleted**, because the *reason* it was wrong is the
reusable lesson: a client comparison that does not control for the thread model is measuring the
thread model. The figure travelled for a day before anyone asked what the in-flight column meant.

## 2026-08-22: how much of this gap survives virtual threads? Nobody has checked, and it may be most of it

**The floors above were measured on 2026-08-21, before virtual threads landed.** Re-reading them
against today's numbers changes what they mean.

**The in-flight column is the tell.** That table's own text notes the Go floor "reaches a concurrency
of 5,000 exactly, where both Java arms plateau near 2,800". Today, on a completely different workload -
100,000 records over ONE partition rather than 500,000 over ten - the shipped PC engine peaked at
**2,841** in flight at the same operating point. The Java floor (`pool`, no engine at all) peaked at
**2,848** yesterday.

**Same ceiling, different days, different partition counts, with and without PC.** It was never a
Parallel Consumer property and it is not a client property either: it is the platform-thread ceiling,
documented in [`perf-platform-threads-are-the-ceiling.md`](perf-platform-threads-are-the-ceiling.md),
and the bare thread-pool arm hits it exactly as hard.

**The Go floor reached 5,000 because goroutines are not platform threads.** Today `core-vt` and
`core-dpvt` both hold 5,000 - and 40,000 when asked for 40,000. **Java has since acquired the answer
to the constraint that produced a large part of this gap**, and none of the numbers above were taken
with it.

### The measurement that must come before any FFI work

Re-run **the Java floor with virtual threads** against the Go floor, at the **same 500,000 records and
ten partitions** as the table above - the conditions matter, and mixing them with today's
single-partition figures is exactly the like-for-like error this repository has already published
once.

- Whatever gap **survives** is genuine client efficiency - protocol implementation, allocation,
  fetch pipelining - and is what an FFI bridge would actually buy.
- Whatever **closes** was the thread model, and was never about Go.

**Until that is run, the size of the prize is unknown**, and it is the only input that decides whether
embedding a Go client is worth its cost. A separate note covers what that cost looks like:
[`next-embedding-franz-go-via-ffi.md`](next-embedding-franz-go-via-ffi.md).

### And one caveat on today's single-partition figures generally

Everything measured on 2026-08-22 used **one partition**, and every arm converged on ~25,000 msg/s
regardless of concurrency or engine. Against yesterday's ten-partition figures - the Java floor alone
did 96,246 msg/s at 0ms - **that plateau is very likely a single-partition fetch ceiling rather than
anything about the engine.** It was recorded as "something upstream of the engine bounds all of them,
and no arm has been profiled at that operating point". This is the likeliest answer, and it is cheap
to test: re-run one cell at ten partitions and see whether the plateau moves.

## The capability case is stronger than expected - and share groups are the reason

**franz-go is the only Go client with KIP-932 share groups.** Kafka made them GA in 4.2.0 on
2026-02-17; franz-go shipped full support 63 days later in v1.21.0. librdkafka has a C-only preview
explicitly not recommended for production and **no Go binding**; Sarama has an unanswered feature
request from 2025.

**That matters to this project more than to most**, because share groups are Kafka's own answer to
part of the problem Parallel Consumer exists to solve, and a competitor's FAQ already routes unordered
workloads to them. Whatever the strategy there, **being able to reach them from a Go proxy is a
capability no other Go client offers at all.**

Other things it has that the Java client does not: **`kfake`**, an in-process protocol-level fake
broker with fault injection - materially better than `MockConsumer` and a serious saving on proxy test
cost; producer data-loss detection callbacks; lag-preferring fetch ordering; AWS MSK IAM SASL built
in; a schema-registry client.

Protocol coverage is close to literal: a ~200-row supported-KIP table from KIP-1 to KIP-1258, and -
unusually - an explicit *"KIPs intentionally not implemented"* section with per-item rationale, whose
exclusions are all Java-idiom or JMX-metric KIPs. Interceptors are present via hooks that **mutate**,
not merely observe, so that is genuine parity rather than a gap.

## The three risks, stated plainly

1. **The bus factor is exactly one.** Not approximately - **90.6% of all commits** are the
   maintainer's (85.5% over the last two years). Second place is 1.0%. **No CONTRIBUTING, no
   CODEOWNERS, no GOVERNANCE, no CODE_OF_CONDUCT.** Funding is one person's GitHub Sponsors.
   Copyright is held personally. He works at Redpanda and Redpanda is a listed adopter, but this is
   personal work under his own copyright and **no corporate maintenance commitment or succession plan
   was found, and it was looked for.**

   **The counterweight is real and should not be waved away**: hygiene beats every alternative -
   **2 open issues**, median time-to-close 9.3 days, against 203 open for confluent-kafka-go and 183
   for segmentio/kafka-go. Today the bus factor reads as a *benefit*: one fastidious expert with total
   context shipped share groups two months after Kafka did, while three better-resourced projects
   shipped nothing. **It is the same fact that becomes an unmaintained dependency the day he stops.**

2. **KIP-848 is gated behind judgement, not backlog.** The new consumer rebalance protocol has been
   implemented since May 2025 and is reachable only via an undocumented magic context string, because
   the maintainer distrusts the broker implementation - repeated heartbeats fencing members far more
   than they should - and says his KAFKA tickets go unanswered. His stated precondition (KIP-1251)
   shipped in Kafka 4.3.0, but the release acting on it has not. **If a supported product must offer
   KIP-848 within the year, that is a dependency on one person changing his mind**, and today only the
   cgo-bound confluent-kafka-go can serve it.

3. **Kafka 4.3 support is written but unshipped.** The PR has sat clean and untouched since
   2026-07-02 - **91 days past Kafka 4.3 GA and counting, the longest lag in a two-year sample** where
   the historical range is 44-89 days and had been *shrinking*. Whether that is bandwidth pressure or
   a deliberate hold is not established.

## Licence: the one axis where it is weaker

**BSD-3-Clause, no CLA, and no express patent grant.** `kafka-clients` is Apache-2.0, which carries an
explicit patent grant and a retaliation clause. Permissiveness is fine either way and neither is
copyleft, so this is not an adoption blocker - but for a project whose own licence posture is a stated
competitive asset ([`next-licensing-strategy.md`](next-licensing-strategy.md)), it is worth knowing
that this dependency is the weaker of the two on patents. The absent CLA also means relicensing would
require tracing 126 contributors, which is a fork-viability question, not ours.

## The alternatives, briefly - and there is no conventional choice

- **confluent-kafka-go** - the only Go client with **KIP-848 GA**, corporately funded, ~6-week
  cadence, commercial support available. Costs: cgo, so `CGO_ENABLED=0` is impossible and truly static
  glibc builds are not available; **no prebuilt binary has GSSAPI/Kerberos**; musl and
  cross-compilation both need special handling. No Go share-group binding.
- **Sarama** - most stars, clean tracker, energetic in 2026, but **also bus factor ~1**, no KIP-848
  (the API key is a commented-out line), and no share groups. It shipped a cooperative-sticky assignor
  in July 2026, i.e. it is catching up to a 2019 KIP.
- **segmentio/kafka-go** - most-imported and **effectively dead**: 2 commits in 12 months, 183 open
  issues, and a Segment employee on the record recommending franz-go instead.

**Installed base and project health point in opposite directions.** Trailing-year commits: franz-go
821, Sarama 381, confluent-kafka-go 126, kafka-go 2.

## What this does and does not argue for

- **It does not argue for rewriting Parallel Consumer's engine.** The measurements say the engine is
  close to free; the client is the large term. A rewrite aimed at the engine would target the smallest
  available saving, which is the same mistake as competing on engine microseconds
  ([`market-analysis-llingr.md`](market-analysis-llingr.md) section 5a).
- **It does argue that a native Go path is worth more than a proxy to the JVM**, for Go users
  specifically, on both throughput and reachable concurrency - and that share groups and `kfake` make
  the case on capability as well as speed.
- **The realistic hedge is a thin internal abstraction over the client**, so a later swap to
  confluent-kafka-go is bounded rather than a rewrite. Cheap to build up front, expensive to retrofit.
  Sponsoring is also cheap relative to the exposure.

## Open questions

1. **Does a Go proxy use franz-go natively, or continue to proxy the JVM engine?** That is the
   language-proxy architecture question (astubbs#242) and this note only supplies inputs to it.
2. ~~What is the project's position on share groups generally?~~ **Answered - and the premise was
   wrong.** An earlier draft of this note claimed no position existed. It does, in two places, and
   both are more developed than anything that would have been written here: `README.adoc` has a
   top-level section *"When to use this library (vs KIP-932 Share Groups)"* with a seven-row
   comparison covering ordering, exactly-once, slow processing, poison messages, broker cost,
   requirements and scaling axis; and `STRATEGY.md` carries the narrow per-record-overhead comparison
   with its cost attached. The settled line is that **the choice is about ordering, not concurrency** -
   share groups scale *out*, Parallel Consumer scales *up*, and what remains uniquely ours is
   key-level ordering with concurrency beyond partition count, plus no processing clock.

   **What follows for franz-go specifically:** share groups are a **separate consumer type and a
   separate group type**, not a mode the existing protocol slips into. A client uses them only by
   calling the share-consumer API deliberately, and the broker side has its own enablement, which
   changed between the 4.0 early-access form and 4.2 GA. **So depending on franz-go does not expose
   anyone to share groups, and does not change any existing behaviour.** They are an *additional
   capability* that no other Go client can offer at all - relevant only if a Go proxy ever wants to
   sit in front of them.
3. **Would virtual threads close any of the Java-versus-Go floor gap?** Untested, and it is the
   pending experiment in the ceiling note. If it does, the urgency of a native Go client drops.
