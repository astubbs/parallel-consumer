# Next: publish the engine comparison, so people actually try the async engines

<!-- inflight-type: next -->
<!-- inflight-impact: coordination -->
<!-- inflight-labels: release-note, needs-measurement -->

Opened 2026-08-21. **The docs present Vert.x, Reactor and Mutiny as integration conveniences - "use
this if you already use that framework".** The measurements say they are a **concurrency-model
decision**, and the difference is large enough that a user picking core by default is very likely
picking the wrong one.

## What there is to show

Measured, same broker, same dataset, same 100ms handler, 5,000 concurrent, the only difference being
whether the work holds a thread:

| | msg/s | Concurrency reached |
|---|---:|---:|
| Core engine | 19,577 | 2,751 of 5,000 |
| **Vert.x engine, async callee** | **32,332** | **5,000 of 5,000** |

And the law underneath it, which is the part worth teaching rather than the ratio:

```
reachable concurrency = min(maxConcurrency, r x handler_latency)
```

where `r` is how fast the machine can activate a platform thread. **Thread-per-record therefore caps
one instance at roughly `r` records per second - about 20,000-27,000 on the machine measured - whatever
the handler's duration.** An engine where the work does not hold a thread has no such term.

## The argument that makes it land, which is not the ratio

**A user on a non-blocking client cannot benefit from it in the core engine, because the API will not
let them:**

```java
void poll(Consumer<PollContext<K, V>> usersVoidConsumptionFunction);
```

Completion is signalled by returning. So someone holding a `CompletableFuture` from an async HTTP
client or a reactive driver must `.join()` it - **parking exactly the thread they chose that library to
avoid.** Their non-blocking stack is wasted, silently, and nothing in the documentation tells them.

**`ExternalEngine` exists so the user can hand back something unfinished.** That is the entire point,
and it is currently buried under "Vert.x integration".

## What is not yet true, and must be before publishing

- **Reactor and Mutiny have never been measured** - `BenchReactor.java.template` exists and is wired
  into nothing, and Mutiny has no arm at all. The family is *assumed* to behave alike because it shares
  `ExternalEngine`. **Do not publish a family claim on one member.** Being addressed on
  `test/bench-all-engine-arms`.
- **The 2.3x is not pure ceiling-removal.** Everything ran on localhost, so the thread-per-request stub
  was also competing for the same twelve cores. Part of that delta is machine relief. **A remote or
  sharded stub is needed before the number is quotable.**
- **32,332 is 64% of theoretical**, against 47,022 for a pure async control - roughly 15,000/s is
  unattributed, and the Vert.x WebClient's unconfigured connection pool is the leading suspect.
  **Publishing a number with a third of it unexplained invites the first reader to find the
  explanation for us.**
- **Every figure is macOS and every handler is a sleep** -
  [`next-benchmark-a-model-of-work-not-work.md`](next-benchmark-a-model-of-work-not-work.md).

## The comparison needs an exactly-once column, not just a throughput one

**`ExternalEngine` rejects transactional commit mode outright.** So every engine this note would steer
users towards **cannot do exactly-once**, and the core engine - the slower one - is the only one that
can.

**A comparison that shows only throughput would be actively misleading**, because it would move
someone off the only engine that supports the guarantee they may be relying on, without ever
mentioning it. The honest framing is a two-axis choice:

| | Concurrency beyond `r x latency` | Exactly-once |
|---|---|---|
| **Core** | no - work holds a thread | **yes** |
| **Async engines** | **yes** | no |

**That is a real trade a reader can make**, and it is more useful than a ratio. It also names the gap
worth closing: an engine that offered both would dominate, and nothing about async completion makes
transactions *impossible* - it makes holding one open across an uncontrolled callback impossible, which
is a narrower statement. See [`next-core-async-user-function.md`](next-core-async-user-function.md).

## Where it goes

- **`README.adoc`** - the engine choice needs to be a decision with a stated criterion, not a list of
  supported frameworks. The criterion is: *does your work block a thread?*
- **The landing page** - dropped 2026-08-22, but its note (in git history) carried the rule that
  no figure is published without the conditions that produced it. This is the first real test of that
  rule.
- **The `ExternalEngine` regression** ([`perf-throughput-regression-since-0-3.md`](perf-throughput-regression-since-0-3.md))
  becomes a release blocker under this framing rather than a curiosity: **it is a tax on the engines
  the documentation would now be steering people towards.**

## 2026-08-24 addendum: two things happened since these figures were taken

Written after the realistic-workload re-take; read before publishing anything above.

1. **Virtual threads changed the comparison.** The table's core figure (19,577 at 2,751 of 5,000)
   is the *platform-thread* core engine - the one the thread-ceiling law binds. `core-vt` reaches
   configured concurrency without an async callee, so "use an async engine to escape the ceiling"
   is no longer the only answer and the Vert.x ratio above overstates today's gap. Re-take the
   table with `core-vt` as a third column before publishing. The law itself
   (`min(maxConcurrency, r x handler_latency)`) is unchanged and is still the durable teaching
   content; what changed is which engines it applies to.
2. **The figures are all-distinct-key `UNORDERED` numbers and must say so.** Per the claims
   decision in
   [`release-v6-phoenix-theme-and-announcement.md`](release-v6-phoenix-theme-and-announcement.md)
   and [`landing-page.yaml`](../data/landing-page.yaml)'s rule, no figure publishes without key
   distribution, per-record delay and concurrency setting. Under Zipf keys with `KEY` ordering the
   key distribution, not the engine, is the binding ceiling - the engine choice this note argues
   for stops mattering long before 5,000 in flight.
