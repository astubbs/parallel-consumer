<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Simplifications considered and declined - do not re-propose without new evidence

From the 2026-08-17 simplification pass over the proxy (astubbs#242): 5 changes applied, 14
candidates declined. **The declined ones are recorded here because the next pass will find them
again**, and re-deriving why each is wrong costs more than reading it. None is deferred work; each is
a decision that the current code is better.

**Where the reason is subtle, the code should carry it too** - a note here stops a human, a comment at
the site stops an automated pass. The three marked **(comment)** below are the ones where the diff
looks strictly better and only the reasoning says otherwise.

Engine seam (`parallel-consumer-proxy/.../engine/`):

- **(comment)** `InFlightRegistry`'s CAS loop → `ConcurrentHashMap.compute`. Proposed as exactly
  equivalent; it is not. `compute` would run the staleness predicate - a call into core's
  `WorkManager` - and log under the map's bin lock, where the current loop is lock-free and
  structurally cannot deadlock against core's locks. The test suite does not catch this.
- An `isEmpty()` guard before the per-pass sweep snapshot. Measured: `Map.copyOf` on an empty map
  returns the shared instance at ~35ns with no allocation. Adds a condition to a liveness-critical
  guard for nothing.
- Hoisting the clock read out of the lease sweep. Tens of nanoseconds, against widening a deliberately
  minimal lease API and changing per-entry "now" semantics.
- A shared named-daemon-thread-factory helper; two three-line lambdas with different naming shapes.
- `isAsyncFutureWork`'s first-element loop → `isEmpty()/get(0)`. The loop mirrors `ReactorProcessor`
  and Vert.x verbatim, and cross-engine consistency outweighs the idiom.
- Consolidating `LivenessLease`'s thrice-written deadline arithmetic; each site is the domain
  definition where it stands.
- Unifying the three sweep/return loops. Each has distinct operational logging - aggregate warn,
  per-record warn, silent - so parameterising trades straight-line code for call-site config.

Transport, config and the Java client:

- **(comment)** Table-driving `OptionsMapper.toOptionsBuilder`'s twenty `hasX()` blocks. Contradicts
  the plan's no-reflection decision for GraalVM reachability, and trades static typing for line count.
- **(comment)** Swapping the hand-written host normaliser for Guava's `HostAndPort`. Guava **throws**
  on bare unbracketed IPv6, which that method deliberately admits - a security-posture change wearing
  a library swap.
- Routing `DirectParallelConsumerClient.poll` through the gRPC client's async wrapper: two
  `CompletableFuture` allocations per record on the in-process hot path, for cosmetic uniformity.
- Extracting a duplicated `sessionEnd()` one-liner; would add public API surface to `-api` to save one
  line.
- A shared logback-capture test helper; the two call sites do different jobs.
- Any change to `AuthorityAllowlistInterceptor` or `SingleConnectionGuard` - security posture, and
  their apparent simplicity is the point.

**The fourteenth was not a rejection but a finding**, and is being fixed rather than recorded: the
same last-committed-offset reverse scan in three modules, which could not be shared from inside the
proxy because core's `CommitHistory.highestCommit()` needs pre-flattened input and exposes only a
Truth `Subject` where the callers need a pollable value.
