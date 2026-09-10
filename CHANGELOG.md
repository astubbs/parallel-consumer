# Change Log

A high level summary of noteworthy changes in each version.

**Dependency bumps.** Routine/automated dependency bumps (e.g. Dependabot) are not listed here. Notable or coordinated dependency refreshes - and any change to a user-facing runtime dependency such as the Kafka client - are summarised under the relevant version, since for a library these affect the transitive dependencies and compatibility that consumers inherit.

**Reference convention (this is a fork).** Links and bare `#NN` / commit hashes refer to this fork, [astubbs/parallel-consumer](https://github.com/astubbs/parallel-consumer). Upstream references are written explicitly as `upstream #NN` and link to [confluentinc/parallel-consumer](https://github.com/confluentinc/parallel-consumer). Entries below `0.6.0.0` predate the fork and their `#NN` refer to upstream.

**How this file is maintained.** A section is frozen once its release has **shipped** - `0.5.x` and below are hand-written legacy from before the fork, and are finished. The section for the release currently being cut is **generated at release time**, replacing whatever text is under it, and is frozen in turn once that release ships. *How* it is generated is not yet decided - `docs/releasing.md` carries the judgement applied when it is written, not a mechanism. **The `0.6.0.0` section below was generated on 2026-09-09 for the tag and is the text that release publishes**; it freezes when the tag is cut. Consequently a PR never adds an entry here and never opens an `## Unreleased` section; the only edit a PR may make is correcting a factual error in text that is already present. The judgement applied when generating (what earns an entry, and how long it may be) is in `docs/releasing.md`.

**The release page body is this file's `## <version>` section, verbatim.** `.github/workflows/release.yml` extracts it before it tags anything and refuses to release if the section is missing or empty, so the heading must be exactly `## <version>` by the time the release is cut.

<!-- git log --pretty="* %s" 0.3.0.2..HEAD -->

## 0.6.0.0

First release of the community fork of [confluentinc/parallel-consumer](https://github.com/confluentinc/parallel-consumer), which is no longer maintained, published to Maven Central as `bz.stub.parallelconsumer`. For most users, upgrading from upstream 0.5.x is the pom and the imports: the Maven groupId and the Java package every import names both change. It is not source-compatible beyond that - the Breaking section below is short and worth reading once. Most of it narrows the internal controller's subclass surface, but a commit that exhausts its budget now throws a PC exception type rather than Kafka's, the JStream result stream now blocks until close, an internal exception is renamed, and `RecordContext` equality is now identity. The committed offset format is unchanged, so an existing consumer group upgrades in place without resetting or migrating offsets. Upstream's last release on Maven Central is 0.5.3.2; its `0.5.3.3` section below was tagged but never published, and everything merged upstream after it - the Mutiny module, the commit-failure log detail, the metrics fix - reaches users here for the first time.

**This is a stability release, and that is the point.** The change set since 0.5.3.3 is the largest this codebase has shipped in one version, and almost all of it is fixes: the commit-path deadlock behind the long-standing "consumption stops after a rebalance" reports, a family of torn reads that lost records silently in every 0.5.x line, the metrics leak, offset accuracy on assignment, an asynchronous commit recorded before the broker answered, and the test lanes that now guard each of them. For the users this library serves, that is the release that matters. The capabilities are queued behind it, and the What comes next section lists them by how far each has got. The bar applied was: every known critical defect resolved, each with a named regression guard that passes. Two known critical defects sit outside that bar, both in the transactional producer mode only, and are named under Known limitations below alongside what the test suite still cannot see. Every fix below was reproduced deterministically and shown to fail before the fix and pass after it; the write-up behind each is in the linked PR.

### Breaking

- **Coordinates and packages** ([#55](https://github.com/astubbs/parallel-consumer/pull/55), [#294](https://github.com/astubbs/parallel-consumer/pull/294)): the Maven groupId `io.confluent.parallelconsumer` is now `bz.stub.parallelconsumer`, and the Java packages `io.confluent.parallelconsumer.*` are now `bz.stub.parallelconsumer.*`, with the shared internal utilities under `bz.stub.parallelconsumer.internal.utils`.
  - For the rename itself, two changes are required: the dependency declaration, and the imports. Rewrite your imports with the one-line `sed` in the README's Upgrading section.
  - The rename changes no signature; the rest of this list is what else can touch your code.
- **A commit that exhausts its budget throws `OffsetCommitBudgetExceededException`** ([#204](https://github.com/astubbs/parallel-consumer/pull/204); [#177](https://github.com/astubbs/parallel-consumer/issues/177), upstream [#833](https://github.com/confluentinc/parallel-consumer/issues/833)). Public package, extends `ParallelConsumerException`, with Kafka's `TimeoutException` or `SaslAuthenticationException` as the cause.
  - Code catching the bare Kafka type around `getFailureCause()` or in a supervisor stops matching: catch the PC type, or unwrap `getCause()`.
  - The message names the budget that ran out, its value, and the option to raise - and says when `offsetCommitTimeout` was below the consumer's `default.api.timeout.ms`, so no retry was reachable.
  - `offsetCommitTimeout` now bounds the whole commit, not each attempt: PC gives up where it used to retry forever.
  - `saslAuthenticationRetryTimeout` counts from the first SASL failure, not from the start of the call.
- **The JStream result `Stream` blocks until the consumer closes** ([#116](https://github.com/astubbs/parallel-consumer/pull/116); upstream [#912](https://github.com/confluentinc/parallel-consumer/issues/912)). It used to end the first time its queue was momentarily empty, usually before the first result, so results piled up behind a caller that had already walked away.
  - Consume the stream on its own thread, as the Vert.x example now does. There is no compatibility path: the old shape never delivered the caller's results.
- **`invalidOffsetMetadataPolicy` governs every unreadable payload, and its default is `IGNORE`, not `FAIL`** ([#207](https://github.com/astubbs/parallel-consumer/pull/207)). An unknown magic byte, a corrupt body, invalid base64 and Kafka Streams metadata all route through the policy; before, most of them threw on assignment whatever it was set to.
  - `FAIL` still stops the consumer, and now does so for every unreadable payload.
- **`batchSize` below one is rejected at construction** ([#496](https://github.com/astubbs/parallel-consumer/pull/496); [#311](https://github.com/astubbs/parallel-consumer/issues/311)). Zero, a negative or null now throws `IllegalArgumentException` from `validate()`, naming the option. Before, zero started a consumer that joined the group, polled, and processed nothing forever with no warning - or, with `messageBufferSize` set, died in a bare `ArithmeticException`. Nothing changes for any value of one or more.
  - A deployment whose batch size comes from a property that resolves to zero now fails to start instead of idling.
- **One exception renamed, one method removed** ([#267](https://github.com/astubbs/parallel-consumer/pull/267)):
  - `InternalRuntimeException` is `PCInternalRuntimeException` - it is what `getFailureCause()` returns, and the old name read like a JDK type in a stack trace.
  - `WorkManager.getSuccessfulWorkListeners()` is gone; use `addSuccessfulWorkListener`. Mutating the returned list from another thread threw `ConcurrentModificationException` inside the control loop and stopped the consumer.
- **`RecordContext` equality is identity** ([#468](https://github.com/astubbs/parallel-consumer/pull/468)). Two contexts built for the same record from different containers no longer compare equal as `Set` members or `Map` keys. It is the fix for a lost record, under Fixes.
- **Subclasses of the internal controller only** ([#296](https://github.com/astubbs/parallel-consumer/pull/296); [#209](https://github.com/astubbs/parallel-consumer/issues/209)) - a user of `ParallelStreamProcessor` is not affected:
  - `setupWorkerPool` must return a pool whose rejection handler is `ThreadPoolExecutor.AbortPolicy`; anything else throws `IllegalArgumentException` at construction. Other handlers were accepted and silently lost records.
  - `setState` is no longer callable from outside its package. A `protected getState` is added.
- **`parallel-consumer-mutiny` requires Java 17** ([#214](https://github.com/astubbs/parallel-consumer/pull/214); [#194](https://github.com/astubbs/parallel-consumer/issues/194), upstream [#906](https://github.com/confluentinc/parallel-consumer/issues/906)) - its real floor, since SmallRye Mutiny 2.x is compiled for 17. Core, Vert.x and Reactor stay at Java 8.

### Improvements

- The project is renamed **Parallel Consumer for Apache Kafka** - the README title, and the `<name>` of the parent and every module ([#276](https://github.com/astubbs/parallel-consumer/pull/276)).
- New `parallel-consumer-mutiny` module - SmallRye Mutiny (Quarkus) integration (upstream [#891](https://github.com/confluentinc/parallel-consumer/pull/891)).
- **Your MDC reaches the threads that run your function.** A `trace_id` or tenant set before `poll*()` is carried into the worker pool and into the Vert.x, Reactor and Mutiny engines, and whatever your function put in the MDC no longer leaks onto the next record's log lines. On by default; `propagateMdc(false)` restores the old behaviour exactly. The captured keys, never the values, are logged once at startup so a request-scoped value pinned at `poll*()` time is discoverable ([#205](https://github.com/astubbs/parallel-consumer/pull/205); [#195](https://github.com/astubbs/parallel-consumer/issues/195), upstream [#907](https://github.com/confluentinc/parallel-consumer/issues/907)).
- **`ParallelConsumerOptions.producerConfig(Map)`** builds PC's own producer from configuration, as an alternative to handing in an instance; supplying both fails validation, and a producer PC built is closed rather than leaked when start-up fails. First step towards surviving producer fencing, which is not in this release ([#426](https://github.com/astubbs/parallel-consumer/pull/426); [#225](https://github.com/astubbs/parallel-consumer/issues/225)).
- **`LongPollingMockConsumer` ships in the main `parallel-consumer-core` artefact**, so a broker-free test of your integration no longer needs the `tests` classifier jar ([#202](https://github.com/astubbs/parallel-consumer/pull/202); [#159](https://github.com/astubbs/parallel-consumer/issues/159), upstream [#526](https://github.com/confluentinc/parallel-consumer/issues/526)).
- New `shards.max.size` gauge: the largest number of records queued behind any single key or partition, so a hot key serialising the work behind it is visible where `shards.size` could not show it (upstream [#905](https://github.com/confluentinc/parallel-consumer/pull/905), via [#57](https://github.com/astubbs/parallel-consumer/pull/57)).
- **A commit-response timeout now tells you what happened.** When the broker-poll thread dies, every committer waiting on it is released at once with the poll thread's own exception as the cause, rather than each waiting out `offsetCommitTimeout` and reporting "Timeout waiting for commit response" - and that message now reports the timeout actually configured, not a constant. When the poll thread is alive, the timeout line states a verdict on whether it is deadlocked, blocked, waiting or merely slow ([#204](https://github.com/astubbs/parallel-consumer/pull/204), [#354](https://github.com/astubbs/parallel-consumer/pull/354)).
- **Vert.x, Reactor and Mutiny throughput.** External engines get the pipelined work request back that a 0.4.0.0 change had removed - the source of a throughput regression against 0.3.x - bounded by a dispatch ceiling so `maxConcurrency` is never breached ([#342](https://github.com/astubbs/parallel-consumer/pull/342)).
- Offset encoding on the commit path no longer walks every offset between the committed base and the highest succeeded one when only run-length encoding is in play - a multi-second scan when a single stuck record left a gap of millions ([#106](https://github.com/astubbs/parallel-consumer/pull/106)).
- **Log lines are bounded.** The commit-failure ERROR names every partition and its offset (upstream [#850](https://github.com/confluentinc/parallel-consumer/pull/850)) but summarises the encoded metadata to its length, the dropped-batch WARN and the user-function-failure ERROR log a summary instead of the whole batch with its keys and values, and each unabridged object moved to DEBUG ([#203](https://github.com/astubbs/parallel-consumer/pull/203), [#428](https://github.com/astubbs/parallel-consumer/pull/428); [#168](https://github.com/astubbs/parallel-consumer/issues/168), [#169](https://github.com/astubbs/parallel-consumer/issues/169), [#170](https://github.com/astubbs/parallel-consumer/issues/170)).
- "Max loading factor steps reached" no longer warns on every control-loop pass. A fixed buffer (`messageBufferSize` set) reports its ceiling once at DEBUG; a dynamic factor that hits its cap warns at most every thirty seconds and says what to raise ([#201](https://github.com/astubbs/parallel-consumer/pull/201); [#155](https://github.com/astubbs/parallel-consumer/issues/155), upstream [#402](https://github.com/confluentinc/parallel-consumer/issues/402)).
- A new consumer group, or one whose committed offset aged out of the offsets topic, no longer logs "Truncating state" at WARN while truncating nothing; it logs at INFO that no committed offset was found and where it starts. The WARN is unchanged for genuine truncation ([#494](https://github.com/astubbs/parallel-consumer/pull/494); [#162](https://github.com/astubbs/parallel-consumer/issues/162), upstream [#546](https://github.com/confluentinc/parallel-consumer/issues/546)).
- The runtime error a user meets when configuring a batch size but calling a non-batch poll method links to this fork's documentation rather than upstream's ([#289](https://github.com/astubbs/parallel-consumer/pull/289)).

### Fixes

#### Priority 1: correctness

Several of these were present in every released 0.5.x line and left no evidence behind: the committed offsets looked correct. Each needs narrow preconditions, and each is persistent once triggered.

- **A commit could describe a partition state that never existed**, because the offset to commit and the encoded note of outstanding offsets above it were read separately, and a completion landing between the reads shifted every encoded offset. The loud form is the reported one - `auto.offset.reset` firing under frequent rebalancing; the quiet form is silent record loss - at traffic at or above the payload width the committed offset tracks the log end exactly, and real records are dismissed as already processed and never retried. Carries upstream's never-merged [#893](https://github.com/confluentinc/parallel-consumer/pull/893) ([#337](https://github.com/astubbs/parallel-consumer/pull/337); [#121](https://github.com/astubbs/parallel-consumer/issues/121), upstream [#894](https://github.com/confluentinc/parallel-consumer/issues/894)).
- **The offset encoder read its range top after its snapshot**, so a completion landing between the two reads encoded every still-incomplete offset in the widened range as complete; on restore they were skipped. Specific to the default commit mode, `PERIODIC_CONSUMER_ASYNCHRONOUS` ([#344](https://github.com/astubbs/parallel-consumer/pull/344)).
- **An asynchronous commit counted as committed when it was sent, not when the broker answered** - the default commit mode again. A failed or dropped acknowledgement left the broker's committed offset behind PC's belief, the `pc.partition.latest.committed.offset` gauge read an offset the broker never had, and the partition's next owner resumed from a position the broker never recorded. Offsets are now marked clean only on the acknowledgement of the exact request that carried them; a transient failure is logged at WARN and re-sent, a permanent one at ERROR ([#470](https://github.com/astubbs/parallel-consumer/pull/470); [#248](https://github.com/astubbs/parallel-consumer/issues/248), upstream [#203](https://github.com/confluentinc/parallel-consumer/issues/203)).
- **A record polled after a rebalance was dropped if a stale container still occupied its offset.** The stale entry from the previous assignment was swept later, but the fresh record was already gone, and Kafka does not redeliver it while the consumer stays up. This is the shape of the "one offset never received, lag climbs, a restart processes it normally" reports ([#31](https://github.com/astubbs/parallel-consumer/pull/31); upstream [#909](https://github.com/confluentinc/parallel-consumer/pull/909), [#183](https://github.com/astubbs/parallel-consumer/issues/183), upstream [#875](https://github.com/confluentinc/parallel-consumer/issues/875)).
- **Two sweeps removed work by offset, so either could evict a fresh replacement racing in from the controller.** The fresh record then sat marked incomplete with nothing to complete it, pinning the partition's commit. `WorkContainer` equality is now identity (see Breaking), so a removal evicts the container it inspected or nothing ([#468](https://github.com/astubbs/parallel-consumer/pull/468), [#492](https://github.com/astubbs/parallel-consumer/pull/492)).
- **A record could be selected and completed before its offset was registered**, which tripped an internal assertion rather than losing anything today, and held only because registration and completion shared a thread ([#450](https://github.com/astubbs/parallel-consumer/pull/450); [#370](https://github.com/astubbs/parallel-consumer/issues/370)).
- **A result from a container that had already been revoked could dirty the freshly assigned partition state**, the one production route into a wrong commit, and its failure-path twin re-queued the stale record where nothing could sweep it ([#346](https://github.com/astubbs/parallel-consumer/pull/346)).
- **A commit rejected because this consumer had already left the group was recorded as done** - no exception, no stall, just bookkeeping ahead of the broker, surfacing when the partition's new owner resumed behind records PC had marked complete ([#108](https://github.com/astubbs/parallel-consumer/pull/108); relates to upstream [#857](https://github.com/confluentinc/parallel-consumer/issues/857)).
- **Corrupt or foreign offset metadata produced answers instead of errors**: a bitset header claiming more bits than the body held fabricated thousands of incomplete offsets, a huge declared length exhausted the heap, a negative run length placed the highest-seen offset below the committed one. A payload is now checked against itself, and a map whose run or bitset extends past the partition's log end offset - which no legitimate map can - is refused at the first batch rather than making PC skip every real record in the claimed range. Both routes take `invalidOffsetMetadataPolicy` (see Breaking). The `IGNORE` recovery also resumed one offset too far, skipping the first record ([#207](https://github.com/astubbs/parallel-consumer/pull/207), [#480](https://github.com/astubbs/parallel-consumer/pull/480)).
- **Transactional mode: exactly-once now holds across a rebalance.** The revocation-time commit ran on the poll thread without draining the controller's mailbox, so it could publish a transaction whose committed offsets omitted records the transaction contained; the partition's next owner reprocessed the input and produced the output again. The revoke now hands its commit to the control thread, bounded by `commitLockAcquisitionTimeout`, and a partition being revoked is fenced from further produces ([#466](https://github.com/astubbs/parallel-consumer/pull/466)).
- **Transactional mode: a terminally failed send no longer publishes half a result set.** The producer callback threw before the client could move the transaction into an abortable state, so the records already accepted were committed and visible to a `read_committed` consumer while the rest never appeared ([#261](https://github.com/astubbs/parallel-consumer/pull/261)).
- **Transactional mode with `batchSize` of two or more redelivered records that had succeeded**, with an ERROR blaming your function on every batch. The produce lock was released once per record for a resource taken once per batch ([#257](https://github.com/astubbs/parallel-consumer/pull/257)).
- **Transactional mode: an `InvalidPidMappingException` on the produce path marked the whole batch succeeded** - output never produced, offset advanced - and a fenced or poisoned producer's abort on close leaked the `KafkaProducer`. The batch now fails and the close always closes the producer ([#429](https://github.com/astubbs/parallel-consumer/pull/429); [#423](https://github.com/astubbs/parallel-consumer/issues/423), upstream [#830](https://github.com/confluentinc/parallel-consumer/issues/830)).
- **A worker pool that silently discarded rejected work no longer passes construction** - see Breaking ([#296](https://github.com/astubbs/parallel-consumer/pull/296)).
- **A record PC could not return to its mailbox after a failure is now fatal to the instance rather than skipped.** Before, it was logged and PC carried on to commit past work that was never done ([#267](https://github.com/astubbs/parallel-consumer/pull/267)).

#### Consumption stopped after a rebalance (upstream [#857](https://github.com/confluentinc/parallel-consumer/issues/857))

Upstream's most-reported symptom - the group goes quiet after a rebalance, with no error, until a restart - was not one defect. Splitting the reports by commit mode, and building a probe that forces the deadlock window open instead of waiting for a randomised run to find it, turned the symptom into a list of mechanisms. Each is closed with its own regression guard:

- **The poll/control commit deadlock**, in `PERIODIC_CONSUMER_SYNC` only: the revocation callback took the commit monitor on the poll thread while the control thread held it and waited for the poll thread. The revoke path now declines the lock instead of blocking on it; partitions are revoked promptly and the offsets re-derived after the rebalance. A fix had existed for four months that nothing could prove, because the test meant to prove it ran a commit mode in which the deadlock cannot occur; the deterministic probe is what settled it, and it runs on every build. The same change removed a per-pass shard scan hidden inside a trace-level log argument, which cost real throughput under `KEY` ordering ([#29](https://github.com/astubbs/parallel-consumer/pull/29); [#119](https://github.com/astubbs/parallel-consumer/issues/119)).
- **A commit landing during a rebalance killed the broker-poll thread**, and the instance died later with a misleading commit timeout. Most likely under the cooperative-sticky assignor, whose members keep committing during rebalances ([#100](https://github.com/astubbs/parallel-consumer/pull/100)).
- **A closing consumer wedged its group**: it stopped polling while draining, so it burned a core and stayed a rebalance-unresponsive member holding its whole assignment ([#80](https://github.com/astubbs/parallel-consumer/pull/80)).
- **A dead broker-poll thread left the Kafka consumer open in the consumer-commit modes** - the shipped default among them - so no LeaveGroup was sent and the dead member kept its partitions until the broker evicted it after `max.poll.interval.ms`: a silent partial outage of about five minutes per instance that died this way. The consumer is now closed in every mode, and the warnings on that path name `max.poll.interval.ms` rather than `session.timeout.ms` as the timeout that actually evicts ([#477](https://github.com/astubbs/parallel-consumer/pull/477)).
- **A rebalance callback could wait, unbounded, on the retry queue's lock** while the control loop scanned it - inside `consumer.poll()`, with the whole group waiting, spent out of `max.poll.interval.ms`. The poll thread no longer touches the retry queue; the controller collects what the callbacks leave ([#481](https://github.com/astubbs/parallel-consumer/pull/481)).
- **The record-intake gate was fed by counters that drifted**: revoking a record parked in retry back-off, or a stale replacement at one offset, left phantom counts that kept the broker poller paused until a restart cleared them. The figure is now derived by conservation from what the maps actually did, with no clamp to hide drift, and the shard's own count is owned by a compare-and-set ([#336](https://github.com/astubbs/parallel-consumer/pull/336), [#373](https://github.com/astubbs/parallel-consumer/pull/373)).
- **A retry entry orphaned by a rebalance kept "work is waiting" true forever** with nothing assigned, and a sibling window left an orphan that held a draining close open to its timeout ([#346](https://github.com/astubbs/parallel-consumer/pull/346), [#437](https://github.com/astubbs/parallel-consumer/pull/437)).
- **Under `KEY` ordering, a success landing mid-revocation could throw out of the rebalance listener** with a `NullPointerException`, killing the poll thread - the third distinct null on that path, after upstream [#757](https://github.com/confluentinc/parallel-consumer/issues/757) ([#345](https://github.com/astubbs/parallel-consumer/pull/345)). A revoke after a failed assignment threw the same way and left later partitions unswept; a missing partition epoch now fails closed with a message naming the invariant ([#451](https://github.com/astubbs/parallel-consumer/pull/451)).
- **Two partition flags crossed threads unfenced.** A stale read of the dirty flag burnt a commit cycle, and on a partition that then went idle the committed offset waited for the next rebalance; a stale read of the back-pressure flag admitted nothing, so nothing succeeded and the encode that would clear it never ran again. Measured on real hardware before being fixed: one is a `volatile`, the other is redesigned as a monotone completion count so nothing clears a flag any more ([#349](https://github.com/astubbs/parallel-consumer/pull/349), [#469](https://github.com/astubbs/parallel-consumer/pull/469)).
- **Reactor: a `Publisher` that completed empty never retired its record**, and one emitting several elements retired it once per element; with the new dispatch ceiling the first would have wedged the engine. Records now retire on the terminal signal ([#342](https://github.com/astubbs/parallel-consumer/pull/342)).
- **The back-pressure pause state is read from Kafka rather than mirrored**, because the eager and cooperative protocols do opposite things with pause state across a rebalance, and a mirror that resets on assignment leaves a cooperative member paused for good. A regression guard, not a live defect on 0.5.x ([#376](https://github.com/astubbs/parallel-consumer/pull/376)).

What was measured and ruled out, so the list above is honest about its edges: the residual stall in very large fleets is the consumer-group protocol's join phase held open by churn, during which every member's poll returns nothing, and PC holds nothing during it ([#444](https://github.com/astubbs/parallel-consumer/pull/444), [#486](https://github.com/astubbs/parallel-consumer/pull/486)); the "eager-mode stall" was a timing bound that flips with the processor count, withdrawn ([#478](https://github.com/astubbs/parallel-consumer/pull/478)); the instance-stall sightings under load are worker saturation with the member busy in user code, not a wedge ([#458](https://github.com/astubbs/parallel-consumer/pull/458), [#488](https://github.com/astubbs/parallel-consumer/pull/488)); and the lag-stagnation detector measures speed rather than liveness, so it now reports instead of failing a run ([#354](https://github.com/astubbs/parallel-consumer/pull/354)). One arm is still unattributed: a member that stops answering the coordinator during a churn storm has been seen only in the randomised chaos suite, never fired under the load experiment, and cannot be told apart from the coordinator's own join phase without the closing members' thread dumps. Its seeds are recorded and [#119](https://github.com/astubbs/parallel-consumer/issues/119) stays open for it.

#### Other fixes

- **`PCMetrics` grew without bound** - one duplicate meter id per commit for the life of the consumer - and an exception from the meter registry during teardown propagated into the poll thread. Registration is deduplicated, revocation removes what it registered, and teardown never throws; `OffsetMapCodecManager` is kept instantiated so meters are no longer recreated on every commit ([#57](https://github.com/astubbs/parallel-consumer/pull/57); [#120](https://github.com/astubbs/parallel-consumer/issues/120), upstream [#859](https://github.com/confluentinc/parallel-consumer/issues/859), upstream [#892](https://github.com/confluentinc/parallel-consumer/pull/892)).
- **Registering a loop-end callback or a success listener from another thread could stop the consumer** with a `ConcurrentModificationException` the handler never reported; both registries are copy-on-write. The same change closed a class: user code that throws where PC's bookkeeping could not survive it. A `retryDelayProvider` that throws, returns null or returns a negative delay now falls back to the configured default instead of leaving its record stuck forever, and a user throwable is rendered without running the user's `getCause()` under the logger ([#267](https://github.com/astubbs/parallel-consumer/pull/267)).
- **A record could be redelivered immediately with its configured back-off skipped**, when a claim decided before a renewed retry delay won a compare-and-set against a state that had cycled back to the same value. The claim is one atomic transition on a per-attempt identity ([#335](https://github.com/astubbs/parallel-consumer/pull/335)).
- **The retry queue read its index without the lock on one path**, and could leave in-flight work queued for retry ([#354](https://github.com/astubbs/parallel-consumer/pull/354)).
- **Transactional mode: an explicit `commitInterval` equal to the default constant was treated as unset** and silently replaced with the 100ms transactional default - fifty times the broker load configured. An explicit interval is now always kept ([#427](https://github.com/astubbs/parallel-consumer/pull/427); [#422](https://github.com/astubbs/parallel-consumer/issues/422)).
- **Offset metadata PC did not write no longer kills the consumer on assignment** - a group previously owned by a Kafka Streams application, another framework or operator tooling crashed the rebalance callback with "Unexpected magic"; it is now routed through the metadata policy above ([#217](https://github.com/astubbs/parallel-consumer/pull/217); [#118](https://github.com/astubbs/parallel-consumer/issues/118), upstream [#326](https://github.com/confluentinc/parallel-consumer/issues/326)).
- **A close racing work distribution no longer kills the control thread**: work is not taken for a pool that is already shut down, a rejection from such a pool is tolerated and the batch left for redelivery, and an instance whose pool died while running closes itself with a recorded failure cause instead of spinning ([#296](https://github.com/astubbs/parallel-consumer/pull/296)).
- **Vert.x: every close entry point now releases the `WebClient` and the Vert.x instance PC built** - only the `close(Duration, DrainingMode)` form did, so a plain `close()` or try-with-resources stranded the event-loop group. A caller-supplied Vert.x runtime is never closed, and the caller's duration bounds the wait again ([#453](https://github.com/astubbs/parallel-consumer/pull/453)).
- The last metrics counter map on the encode path is concurrent, like its siblings ([#452](https://github.com/astubbs/parallel-consumer/pull/452)).
- Records polled for a partition whose assignment callback has not yet run are skipped and redelivered rather than raising a `NullPointerException` on a null epoch ([afde8c5e](https://github.com/astubbs/parallel-consumer/commit/afde8c5e)).

### Known limitations

Stated so the release does not claim more than it can show. None is new in this release.

- **Transactional producer mode: a revocation can wait on the transaction lock past the poll interval.** When a second instance joins while a long commit holds the lock, the revocation callback waits for it - since [#466](https://github.com/astubbs/parallel-consumer/pull/466) bounded by `commitLockAcquisitionTimeout` rather than unbounded, but not yet declined - and a wait longer than `max.poll.interval.ms` evicts the member. This is upstream's one verified-bug report. The fix depends on producer-fencing recovery and follows this release ([#44](https://github.com/astubbs/parallel-consumer/issues/44), upstream [#803](https://github.com/confluentinc/parallel-consumer/issues/803); fix in [#408](https://github.com/astubbs/parallel-consumer/pull/408)).
- **Transactional producer mode: a poisoned transaction is not aborted while the instance runs.** One terminal send failure - typically a record the broker rejects as too large - leaves the transaction open, so that partition stops for the life of the process. No partial result set is published; the data guarantee holds. The fix is on the same stack and follows this release ([#434](https://github.com/astubbs/parallel-consumer/pull/434)).
- **Any instance holding records that never succeed will eventually stop fetching, in any commit mode.** The intake gate counts records that are in the system but parked for retry, and once the population of retrying records outgrows what the retry service is holding in back-off, the gate reads "loaded", every partition is paused, and nothing unpauses it - successes flow for a while and then freeze, with the workers mostly idle, at a threshold you can compute from `messageBufferSize` and the retry delay. It needs neither a high failure rate nor saturated workers, and it is the best explanation yet for the flat processed-record counters in the commit-response-timeout reports. This release makes the state visible: a WARN when the gate has stayed latched with nothing retiring ([#497](https://github.com/astubbs/parallel-consumer/pull/497)); before it, the only signal was the paused-partition gauge. Two workarounds today: handle terminal failures inside your function so the record retires, as the README's Skipping Records section describes, or a `retryDelayProvider` with a growing back-off, which delays the latch. The fix is the dead-letter queue, under What comes next ([#487](https://github.com/astubbs/parallel-consumer/pull/487); [#149](https://github.com/astubbs/parallel-consumer/issues/149), upstream [#310](https://github.com/confluentinc/parallel-consumer/issues/310)).
  <!--
  TAG-DAY: astubbs#497 must be merged before the tag. If it is not, delete the WARN sentence above and put
  "silently" back into the bullet's first sentence.
  -->
- **A fenced producer still terminates the instance.** Recovery - abort, rebuild the producer, rejoin - is designed and follows this release ([#225](https://github.com/astubbs/parallel-consumer/issues/225)).
- **The test suite cannot see a single wedged key-order shard inside an otherwise healthy partition.** A key whose work will never be dispatched again pins that partition's commit and fails no detector; a per-partition commit-liveness gate now exists ([#491](https://github.com/astubbs/parallel-consumer/pull/491)), the per-shard half does not, and no reproduction of the shape has been found.
- **The commit-response timeout reports were never reproduced** (upstream [#809](https://github.com/confluentinc/parallel-consumer/issues/809), [#833](https://github.com/confluentinc/parallel-consumer/issues/833); [#175](https://github.com/astubbs/parallel-consumer/issues/175)). Every known cause of that message is fixed above, the message now says which it was, and a soak built to hunt the timeout found the intake stall instead.
- **A revocation redelivers work that was in flight - by design.** PC commits only completed work, so records mid-flight when a partition moves are processed again by the new owner. The README section on reducing duplicate replay - choose the cooperative assignor, and drain on close - is the guidance (upstream [#777](https://github.com/confluentinc/parallel-consumer/issues/777)).
- The JStream result queue has no capacity bound, so a consumer of the stream slower than the producer grows it; what this release fixed is the consumer that walked away ([#216](https://github.com/astubbs/parallel-consumer/issues/216)).

### What comes next

This release ships fixes. The capabilities are behind it in a queue, and this is that queue's state at the tag, grouped by how far each item has got. The roadmap data is `docs/data/roadmap.yaml`, rendered into the README. Nothing here is a date, and everything marked preview ships behind an opt-in.

**Implemented, in the merge queue** - the code exists on an open pull request, awaiting review and merge:

- **Producer-fencing recovery**: abort, rebuild the producer and carry on instead of dying ([#410](https://github.com/astubbs/parallel-consumer/pull/410), on [#472](https://github.com/astubbs/parallel-consumer/pull/472) and [#474](https://github.com/astubbs/parallel-consumer/pull/474); [#225](https://github.com/astubbs/parallel-consumer/issues/225)). With it, the two transactional-mode limitations above close: a revocation declines the transaction lock ([#408](https://github.com/astubbs/parallel-consumer/pull/408)) and a poisoned transaction is aborted ([#434](https://github.com/astubbs/parallel-consumer/pull/434)).
- **Virtual threads** for the user function, opt-in, JDK 21 and later ([#360](https://github.com/astubbs/parallel-consumer/pull/360); [#190](https://github.com/astubbs/parallel-consumer/issues/190)).
- **Self-tuning concurrency**: the engine discovers its own sustainable admission target instead of asking you to guess `maxConcurrency` ([#333](https://github.com/astubbs/parallel-consumer/pull/333); [#227](https://github.com/astubbs/parallel-consumer/issues/227)).
- **Global rate limiting** across a whole consumer group, so a downstream budget is respected by every instance together: the partition-share allocator ([#456](https://github.com/astubbs/parallel-consumer/pull/456)) and its first cut ([#392](https://github.com/astubbs/parallel-consumer/pull/392); [#228](https://github.com/astubbs/parallel-consumer/issues/228)).
- **Kafka Streams on Parallel Consumer, preview**: a Streams topology run with per-key concurrency behind an opt-in switch ([#271](https://github.com/astubbs/parallel-consumer/pull/271), [#388](https://github.com/astubbs/parallel-consumer/pull/388)), with worker failures and buffer pressure delivered into Streams' own control paths ([#395](https://github.com/astubbs/parallel-consumer/pull/395)), task ownership carried through rebalance ([#394](https://github.com/astubbs/parallel-consumer/pull/394)), stream time ([#396](https://github.com/astubbs/parallel-consumer/pull/396)), a refusal of the surface it cannot run safely ([#389](https://github.com/astubbs/parallel-consumer/pull/389)), an evidence suite ([#398](https://github.com/astubbs/parallel-consumer/pull/398)) and a runnable example ([#391](https://github.com/astubbs/parallel-consumer/pull/391)); [#255](https://github.com/astubbs/parallel-consumer/issues/255).
- **Kafka Connect sinks on Parallel Consumer, preview** ([#269](https://github.com/astubbs/parallel-consumer/pull/269); [#240](https://github.com/astubbs/parallel-consumer/issues/240)).
- **Other languages**: key-ordered concurrency for non-JVM runtimes through a sidecar, with clients for Python, Go, Rust, Ruby, .NET, TypeScript, Swift and C++ - the frozen wire contract ([#383](https://github.com/astubbs/parallel-consumer/pull/383)), the server ([#384](https://github.com/astubbs/parallel-consumer/pull/384)), a GraalVM native image ([#385](https://github.com/astubbs/parallel-consumer/pull/385)), the Java reference client ([#386](https://github.com/astubbs/parallel-consumer/pull/386)), one conformance suite every client must pass ([#387](https://github.com/astubbs/parallel-consumer/pull/387)), the per-language clients ([#390](https://github.com/astubbs/parallel-consumer/pull/390)) and demos ([#331](https://github.com/astubbs/parallel-consumer/pull/331)), and an in-process path for Go, Python and C with no sidecar at all ([#340](https://github.com/astubbs/parallel-consumer/pull/340)); [#242](https://github.com/astubbs/parallel-consumer/issues/242).
- **A commit-failure seam**: the application decides what happens when a commit fails, instead of PC always terminating ([#352](https://github.com/astubbs/parallel-consumer/pull/352); [#317](https://github.com/astubbs/parallel-consumer/issues/317)).
- **A health-check surface** on the consumer interface ([#226](https://github.com/astubbs/parallel-consumer/pull/226)).
- **Observability**: record residence time, how long a record spends inside PC end to end including retries ([#359](https://github.com/astubbs/parallel-consumer/pull/359)); an embedded web dashboard showing offset encoding and in-flight state ([#268](https://github.com/astubbs/parallel-consumer/pull/268); [#215](https://github.com/astubbs/parallel-consumer/issues/215)); denser offset metadata ([#306](https://github.com/astubbs/parallel-consumer/pull/306); [#192](https://github.com/astubbs/parallel-consumer/issues/192)).
- **Engine performance**: a direct-pull engine with a constant-time scan ([#361](https://github.com/astubbs/parallel-consumer/pull/361)) and the benchmark harness and results behind it ([#362](https://github.com/astubbs/parallel-consumer/pull/362)).
- **Documentation**: a versioned documentation site ([#302](https://github.com/astubbs/parallel-consumer/pull/302), [#316](https://github.com/astubbs/parallel-consumer/pull/316); [#208](https://github.com/astubbs/parallel-consumer/issues/208)), a README per module ([#303](https://github.com/astubbs/parallel-consumer/pull/303)), and industry-grounded examples for every module ([#266](https://github.com/astubbs/parallel-consumer/pull/266)).
- **An API-compatibility gate**, so the published Java API cannot change unnoticed ([#315](https://github.com/astubbs/parallel-consumer/pull/315)).

**Designed, not yet built** - requirements or a design exist, and no implementation does:

- **A dead-letter queue** for records that cannot succeed - the most-asked feature, and the proper fix for the retry-forever limitation above ([#149](https://github.com/astubbs/parallel-consumer/issues/149), brainstorm [#313](https://github.com/astubbs/parallel-consumer/pull/313), upstream [#310](https://github.com/confluentinc/parallel-consumer/issues/310)).
- **Batch failure attribution**: a batch function reports which records failed, so only those retry ([#189](https://github.com/astubbs/parallel-consumer/issues/189)).
- **An error seam on the poll path**, so a record that cannot be deserialised can be skipped or diverted rather than killing the instance ([#163](https://github.com/astubbs/parallel-consumer/issues/163), [#148](https://github.com/astubbs/parallel-consumer/issues/148), [#153](https://github.com/astubbs/parallel-consumer/issues/153)).
- **Micro-batching** by size and time rather than by whatever is available, once the batch-composition decision it depends on is taken (upstream [#915](https://github.com/confluentinc/parallel-consumer/pull/915) is waiting on the same decision).
- **Bounded internal buffers** with visible depth, and **delivered-value metrics** - the measures the library exists to improve, emitted directly.
- **A Java 17 baseline with Kafka 4 support**, for the 0.7 line ([#53](https://github.com/astubbs/parallel-consumer/pull/53)).

**Toward 1.0**: settle the public API so it stops moving, including a stated thread-safety contract ([#139](https://github.com/astubbs/parallel-consumer/issues/139)).

### Dependencies

- Dependencies and build plugins refreshed to their latest non-major versions ([#73](https://github.com/astubbs/parallel-consumer/pull/73)): JUnit 5.10.2 → 5.14.4, Mockito 5.12.0 → 5.23.0, Testcontainers 1.19.8 → 1.21.4, AssertJ 3.24.2 → 3.27.7, SLF4J 2.0.13 → 2.0.18, Project Reactor 3.6.2 → 3.8.7 (the last step closing two advisories), SmallRye Mutiny 2.9.4 → 2.9.5, Vert.x 4.5.7 → 4.5.31, Lombok 1.18.28 → 1.18.46, Logback 1.5.x → 1.6.1, plus Guava, commons-lang3 and the PostgreSQL driver.
- The Kafka client moves 3.9.1 → **3.9.2**, a patch bump within the same 3.9 line - no API or behaviour change for callers. `lz4-java` is pinned ahead of the version kafka-clients 3.9.2 ships, which carries an advisory in code this library never calls; the pin drops once the client ships the fix itself.
- Netty is held at 4.1.137.Final through `netty-bom`, closing an advisory in the version Vert.x resolves on its own; this reaches users of `parallel-consumer-vertx` transitively.
- **Micrometer is held on the 1.13 line (1.13.15), with five advisories excluded in the pom** - three against it, two against its transitive `HdrHistogram` and `LatencyUtils`. Each is argued unreachable from this library in the pom itself - the affected emitters and instrumentation are not on the classpath, and no tag PC emits can carry the tainted input - and none has a public fix on the 1.13 line. The move to 1.16.7 or later, which retires all of them, waits on the Prometheus registry package rename in the metrics example. A user who hands PC a StatsD or logging registry together with attacker-controlled common tags is in the advisory's own antipattern and should move to 1.16.7 themselves. The reasoning per advisory: [docs/inflight/deps-cve-backlog.md](https://github.com/astubbs/parallel-consumer/blob/v0.6.0.0/docs/inflight/deps-cve-backlog.md) ([#281](https://github.com/astubbs/parallel-consumer/pull/281), [#430](https://github.com/astubbs/parallel-consumer/pull/430), [#445](https://github.com/astubbs/parallel-consumer/pull/445), [#493](https://github.com/astubbs/parallel-consumer/pull/493)).
- Kafka 4.x, JUnit 6, Testcontainers 2, Vert.x 5, Mutiny 3 and WireMock 3 were deliberately deferred to keep the release low-risk; each has its blocker recorded in [docs/inflight/deps-deferred-majors.md](https://github.com/astubbs/parallel-consumer/blob/v0.6.0.0/docs/inflight/deps-deferred-majors.md).

### Examples

- `parallel-consumer-example-streams`: `StreamsApp` now takes `bootstrapServers` in its constructor and is actually runnable - its consumer, producer and server methods were stubs that only the test filled in.
- The README's custom retry-delay example halved the wait on every failed attempt instead of growing it; the multiplier is corrected in the source the README is generated from, so upstream's docs-only patch cannot be lost again ([#196](https://github.com/astubbs/parallel-consumer/pull/196); [#167](https://github.com/astubbs/parallel-consumer/issues/167), upstream [#622](https://github.com/confluentinc/parallel-consumer/issues/622)).
- The Vert.x example consumes the JStream result stream on its own thread, as the API now requires ([#116](https://github.com/astubbs/parallel-consumer/pull/116)).

### Build & CI

Test and CI infrastructure was rebuilt while preparing this release. The parts that say something about how carefully the library is tested:

- **A flake fails the build.** The surefire retry that turned a failed-then-passed test into a green run is gone; what it had been hiding is tracked in a ledger, and a test that is genuinely under diagnosis is quarantined - annotated, registered, and tracked to an owning fix PR, with CI enforcing the registry in both directions and a release blocked while any test is quarantined. The registry is empty at this release: its last entries were cleared by fixing what they were about ([#224](https://github.com/astubbs/parallel-consumer/pull/224), [#84](https://github.com/astubbs/parallel-consumer/pull/84), [#473](https://github.com/astubbs/parallel-consumer/pull/473)).
- **Chaos Pain Suite** - seeded, replayable scenarios that drive consumer fleets through rebalance storms under no-loss, bounded-duplicate and progress SLOs, with an ambient probe that autopsies every failing broker test, built to catch the silent stalls behind upstream [#857](https://github.com/confluentinc/parallel-consumer/issues/857). Several of the fixes above were found or confirmed this way ([#83](https://github.com/astubbs/parallel-consumer/pull/83), [#86](https://github.com/astubbs/parallel-consumer/pull/86)).
- **Concurrency testing under a controlled scheduler, as standing lanes.** Lincheck harnesses over the shard, retry-queue and partition-state seams, calibrated by refinding most of the torn-read defects above unaided - and one nobody had named - and a standalone jcstress module that measured the memory-model residuals on real hardware before they were fixed ([#347](https://github.com/astubbs/parallel-consumer/pull/347), [#404](https://github.com/astubbs/parallel-consumer/pull/404), [#348](https://github.com/astubbs/parallel-consumer/pull/348)). Next in this lane is Fray, which explores thread interleavings deterministically over the whole engine rather than a seam at a time.
- **A machine-checked register of every documented transactional guarantee**, each with the test that proves it, which refuses to certify a run whose filters deselected its proofs ([#262](https://github.com/astubbs/parallel-consumer/pull/262), [#443](https://github.com/astubbs/parallel-consumer/pull/443)).
- **No silently uncollected tests.** An ArchUnit rule fails the build when a class holding tests is named something surefire never runs - dormant classes were found this way and re-enabled - and a second keeps Docker-dependent tests out of the fast unit suite. The tests inherited disabled from upstream are re-enabled or deleted, so no test in the tree is `@Disabled` ([#101](https://github.com/astubbs/parallel-consumer/pull/101), [#264](https://github.com/astubbs/parallel-consumer/pull/264)).
- **Static analysis on by default, as a required check.** SpotBugs over every module including tests, with fb-contrib, find-sec-bugs and the SLF4J detectors; Error Prone with NullAway and its `@GuardedBy` lock-discipline check at error level; Infer with every Java checker on - RacerD's race analysis among them - as a ratchet against a known-findings set, so a new finding fails and an old one cannot silently return; javac's own lint; and forbidden-apis. Every disabled rule is registered with a reason and a re-enable trigger ([#356](https://github.com/astubbs/parallel-consumer/pull/356)).
- **GitHub code scanning** (CodeQL) over the Java, the workflows and the tooling on every PR, and **dependency review** on every dependency change.
- **Mutation testing** on the classes a PR changes, as its own non-blocking check, plus a nightly whole-repo sweep; and forked-per-broker integration tests that removed a long-standing flake class ([#111](https://github.com/astubbs/parallel-consumer/pull/111), [#463](https://github.com/astubbs/parallel-consumer/pull/463), [#68](https://github.com/astubbs/parallel-consumer/pull/68)).
- **Coverage per suite, with history.** Unit and integration coverage are uploaded under separate flags with patch and project checks on every PR, every master push uploads its base, and a query tool reads that history to say whether a given test has ever failed and at which commit - a record that outlives any CI log.
- **Throughput measured against master, not against a number in a file.** Every PR's performance test is compared with recent master runs read from their artefacts, the run-to-run spread is reported beside the ratio so a reading is not mistaken for a result, and a master-side baseline series exists for the first time ([#401](https://github.com/astubbs/parallel-consumer/pull/401)).
- **Duplicate-code reports on every PR** from two engines cross-validating each other, gated on the increase against the base branch rather than an absolute; and a file-similarity report for near-copies the line-level engines miss.
- **A whole-tree CVE scan that can prove it ran**, on every PR and weekly, as its own check that goes red on a finding without blocking a merge; every exclusion carries its reasoning and a retirement condition in the pom ([#279](https://github.com/astubbs/parallel-consumer/pull/279), [#489](https://github.com/astubbs/parallel-consumer/pull/489)).
- **Repository hygiene as a required check.** Every issue reference must name its repository, because the fork's issue numbers overlap upstream's; every cited file path must exist; copyright headers, the documentation data's structure, shell lint and the PR checklist are all gated; and a stacked PR cannot merge before its parents.

<!--
There is no 0.5.3.4 release. The upstream #892 metrics fix once tracked under a "0.5.3.4" heading here was
never published upstream as 0.5.3.4; it ships in this fork's 0.6.0.0 (folded into the Fixes above).
-->

## 0.5.3.3

### Fixes

- fix: close parallel consumer on transactional mode when InvalidPidMappingException (#830)
- fix: support kafka-clients 3.9.0 (#841)
- fix: Paused consumption across multiple consumers (#857)

## 0.5.3.2

### Fixes

- fix: include inflight message count in polling backpressure logic (#836)
- fix: message loss on closing or partitions revoked (#827) fixes (#826)
- fix: unbounded retry queue growth preventing polling from being throttled and leading to OOM (#834) fixes (#832, #817)

### Note
#836 introduces a change in how buffer size is calculated as now inflight messages are counted as part of buffer size - so behaviour of existing applications may change and pausing of consumer happen sooner.

## 0.5.3.1

### Fixes

- fix: ConcurrentModificationException Happened while high load and draining (#822) fixes (#821)
- fix: safely completing doClose() (#818) partially fixes (#809)
- Improved offset commit retry. Add support for SaslAuthenticationException retry timeout (#819), partially fixes (#809) in Commit_Sync mode

## 0.5.3.0

### Fixes

- fix: ReactorProcessor - run used-defined function in provided scheduler rather than in pc-pool thread (#798 / #794), fixes (#793)
- fix: fix issue for cannot close and exit properly when re-balancing storm (#787)
- fix: Support for PCRetriableException in ReactorProcessor (#733)
- fix: NullPointerException on partitions revoked (#757)
- fix: remove lingeringOnCommitWouldBeBeneficial and unused imports (#732)
- fix: Fix failing auto-commit check for kafka-clients >= v3.7.0 (#721)
- fix: Fix redundant rebalance callback in LongPollingMockConsumer for Kafka >= 3.6 (#765)

### Improvements

- improvement: stale containers exclusion and handling improvement (#779)
- improvement: add multiple caches for accelerating available container count calculation （#667）
- improvement: RecordContext now exposes lastFailureReason (#725)

### Dependencies

- build(deps): Bump Kafka to 3.6.2
- build(deps): Bump Kafka to 3.7.0

## 0.5.2.8

### Fixes

- fix: Fix equality and hash code for ShardKey with array key (#638), resolves (#579)
- fix: Fix target loading computation for inflight records (#662)
- fix: Fix synchronisation logic for transactional producer commit affecting non-transactional usage (#665), resolves (#637)
- fix: Fix for race condition in partition state clean/dirty tracking (#666), resolves (#664)

### Improvements

- feature: Make PC message buffer size configurable - two new configuration options for controlling buffer size added (#682)

## 0.5.2.7

### Fixes

- fix: Return cached pausedPartitionSet (#620), resolves (#618)
- fix: Parallel consumer stops processing data sometimes (#623), fixes (#606)
- fix: Add synchronization to ensure proper intializaiton and closing of PCMetrics singleton (#627), fixes (#617)
- fix: Readme - metrics example correction (#614)
- fix: Remove micrometer-atlas dependency (#628), fixes (#625)

### Improvements

- Refactored metrics implementation to not use singleton - improves meter separation, allows correct metrics subsystem operation when multiple parallel consumer instances are running in same java process (#630), fixes (#617) improves on (#627)

## 0.5.2.6

### Improvements

- feature: Micrometer metrics (#594)
- feature: Adds an option to pass an invalid offset metadata error policy (#537), improves (#326)
- feature: Lazy intialization of workerThreadPool (#531)

### Fixes

- fix: Don't drain mode shutdown kills inflight threads (#559)
- fix: Drain mode shutdown doesn't pause consumption correctly (#552)
- fix: RunLength offset decoding returns 0 base offset after no-progress commit - related to (#546)
- fix: Transactional PConsumer stuck while rebalancing - related to (#541)

### Dependencies

- PL-211: Update dependencies from dependabot, Add mvnw, use mvnw in jenkins (#583)
- PL-211: Update dependencies from dependabot (#589)

## 0.5.2.5

### Fixes

- fixes: #195 NoSuchFieldException when using consumer inherited from KafkaConsumer (#469)
- fix: After new performance fix PR#530 merges - corner case could cause out of order processing (#534)
- fix: Cleanup WorkManager's count of in-progress work, when work is stale after partition revocation (#547)

### Improvements

- perf: Adds a caching layer to work management to alleviate O(n) counting (#530)

## 0.5.2.4

### Improvements

- feature: Simple PCRetriableException to remove error spam from logs (#444)
- minor: fixes #486: Missing generics in JStreamParallelStreamProcessor #491
- minor: partially address #459: Moves isClosedOrFailed into top level ParallelConsumer interface (#491)
- tests: Demonstrates how to use MockConsumer with PC for issue #176
- other minor improvements

### Fixes

- fixes #409: Adds support for compacted topics and commit offset resetting (#425)
  - Truncate the offset state when bootstrap polled offset higher or lower than committed
  - Prune missing records from the tracked incomplete offset state, when they're missing from polled batches
- fix: Improvements to encoding ranges (int vs long) #439
  - Replace integer offset references with long - use Long everywhere we deal with offsets, and where we truncate down, do it exactly, detect and handle truncation issues.

## 0.5.2.3

### Improvements

- Transactional commit mode system improvements and docs (#355)
  - Clarifies transaction system with much better documentation.
  - Fixes a potential race condition which could cause offset leaks between transactions boundaries.
  - Introduces lock acquisition timeouts.
  - Fixes a potential issue with removing records from the retry queue incorrectly, by having an inconsistency between compareTo and equals in the retry TreeMap.
- Adds a very simple Dependency Injection system modeled on Dagger (#398)
- Various refactorings e.g. new ProducerWrap

- Dependencies
  - build(deps): prod: zstd, reactor, dev: podam, progressbar, postgresql maven-plugins: versions, help (#420)
  - build(deps-dev): bump postgresql from 42.4.1 to 42.5.0
  - bump podam, progressbar, zstd, reactor
  - build(deps): bump versions-maven-plugin from 2.11.0 to 2.12.0
  - build(deps): bump maven-help-plugin from 3.2.0 to 3.3.0
  - build(deps-dev): bump Confluent Platform Kafka Broker to 7.2.2 (#421)
  - build(deps): Upgrade to AK 3.3.0 (#309)

### Fixes

- fixes #419: NoSuchElementException during race condition in PartitionState (#422)
- Fixes #412: ClassCastException with retryDelayProvider (#417)
- fixes ShardManager retryQueue ordering and set issues due to poor Comparator implementation (#423)

## v0.5.2.2

### Fixes

- Fixes dependency scope for Mockito from compile to test (#376)

## v0.5.2.1

### Fixes

- Fixes regression issue with order of state truncation vs commit (#362)

## v0.5.2.0

### Fixes and Improvements

- fixes #184: Fix multi topic subscription with KEY order by adding topic to shard key (#315)
- fixes #329: Committing around transaction markers causes encoder to crash (#328)
- build: Upgrade Truth-Generator to 0.1.1 for user Subject discovery (#332)

### Build

- build: Allow snapshots locally, fail in CI (#331)
- build: OSS Index scan change to warn only and exclude Guava CVE-2020-8908 as it's WONT_FIX (#330)

### Dependencies

- build(deps): bump reactor-core from 3.4.19 to 3.4.21 (#344)
- build(deps): dependabot bump Mockito, Surefire, Reactor, AssertJ, Release (#342) (#342)
- build(deps): dependabot bump TestContainers, Vert.x, Enforcer, Versions, JUnit, Postgress (#336)

### Linked issues

- Message with null key lead to continuous failure when using KEY ordering #318
- Subscribing to two or more topics with KEY ordering, results in messages of the same Key never being processed #184
- Cannot have negative length BitSet error - committing transaction adjacent offsets #329

## v0.5.1.0

### Features

- #193: Pause / Resume PC (circuit breaker) without unsubscribing from topics

### Fixes and Improvements

- #225: Build and runtime support for Java 16+ (#289)
- #306: Change Truth-Generator dependency from compile to test
- #298: Improve PollAndProduce performance by first producing all records, and then waiting for the produce results.Previously, this was done for each ProduceRecord individually.

## v0.5.0.0

### Features

- feature: Poll Context object for API (#223)
  - PollContext API - provides central access to result set with various convenience methods as well as metadata about records, such as failure count
- major: Batching feature and Event system improvements
  - Batching - all API methods now support batching. See the Options class set batch size for more information.

### Fixes and Improvements

- Event system - better CPU usage in control thread
- Concurrency stability improvements
- Update dependencies
- #247: Adopt Truth-Generator (#249)
  - Adopt [Truth Generator](https://github.com/astubbs/truth-generator) for automatic generation of [Google Truth](https://truth.dev/) Subjects
- Large rewrite of internal architecture for improved maintence and simplicity which fixed some corner case issues
  - refactor: Rename PartitionMonitor to PartitionStateManager (#269)
  - refactor: Queue unification (#219)
  - refactor: Partition state tracking instead of search (#218)
  - refactor: Processing Shard object
- fix: Concurrency and State improvements (#190)

### Build

- build: Lock TruthGenerator to 0.1 (#272)
- build: Deploy SNAPSHOTS to maven central snaphots repo (#265)
- build: Update Kafka to 3.1.0 (#229)
- build: Crank up Enforcer rules and turn on ossindex audit
- build: Fix logback dependency back to stable
- build: Upgrade TestContainer and CP

## v0.4.0.1

### Improvements

- Add option to specify timeout for how long to wait offset commits in periodic-consumer-sync commit-mode
- Add option to specify timeout for how long to wait for blocking Producer#send

### Docs

- docs: Confluent Cloud configuration links
- docs: Add Confluent's product page for PC to README
- docs: Add head of line blocking to README

## v0.4.0.0
<!-- https://github.com/confluentinc/parallel-consumer/releases/tag/0.4.0.0 -->

### Features

- [Project Reactor](https://projectreactor.io/) non-blocking threading adapter module
- Generic Vert.x Future support - i.e. FileSystem, db etc...

### Fixes and Improvements

- Vert.x concurrency control via WebClient host limits fixed - see #maxCurrency
- Vert.x API cleanup of invalid usage
- Out of bounds for empty collections
- Use ConcurrentSkipListMap instead of TreeMap to prevent concurrency issues under high pressure
- log: Show record topic in slow-work warning message

## v0.3.2.0

### Fixes and Improvements

- Major: Upgrade to Apache Kafka 2.8 (still compatible with 2.6 and 2.7 though)
- Adds support for managed executor service (Java EE Compatibility feature)
- #65 support for custom retry delay providers

## v0.3.1.0

### Fixes and Improvements

- Major refactor to code base - primarily the two large God classes
  - Partition state now tracked separately
  - Code moved into packages
- Busy spin in some cases fixed (lower CPU usage)
- Reduce use of static data for test assertions - remaining identified for later removal
- Various fixes for parallel testing stability

## v0.3.0.3

### Fixes and Improvements

#### Overview

- Tests now run in parallel
- License fixing / updating and code formatting
- License format runs properly now when local, check on CI
- Fix running on Windows and Linux
- Fix JAVA_HOME issues

#### Details:

- tests: Enable the fail fast feature now that it's merged upstream
- tests: Turn on parallel test runs
- format: Format license, fix placement
- format: Apply Idea formatting (fix license layout)
- format: Update mycila license-plugin
- test: Disable redundant vert.x test - too complicated to fix for little gain
- test: Fix thread counting test by closing PC @After
- test: Test bug due to static state overrides when run as a suite
- format: Apply license format and run every All Idea build
- format: Organise imports
- fix: Apply license format when in dev laptops - CI only checks
- fix: javadoc command for various OS and envs when JAVA_HOME missing
- fix: By default, correctly run time JVM as jvm.location

## v0.3.0.2

### Fixes and Improvements

- ci: Add CODEOWNER
- fix: #101 Validate GroupId is configured on managed consumer
- Use 8B1DA6120C2BF624 GPG Key For Signing
- ci: Bump jdk8 version path
- fix: #97 Vert.x thread and connection pools setup incorrect
- Disable Travis and Codecov
- ci: Apache Kafka and JDK build matrix
- fix: Set Serdes for MockProducer for AK 2.7 partition fix KAFKA-10503 to fix new NPE
- Only log slow message warnings periodically, once per sweep
- Upgrade Kafka container version to 6.0.2
- Clean up stalled message warning logs
- Reduce log-level if no results are returned from user-function (warn -> debug)
- Enable java 8 Github
- Fixes #87 - Upgrade UniJ version for UnsupportedClassVersion error
- Bump TestContainers to stable release to specifically fix #3574
- Clarify offset management capabilities

## v0.3.0.1

- fixes #62: Off by one error when restoring offsets when no offsets are encoded in metadata
- fix: Actually skip work that is found as stale

## v0.3.0.0

### Features

- Queueing and pressure system now self tuning, performance over default old tuning values (`softMaxNumberMessagesBeyondBaseCommitOffset` and `maxMessagesToQueue`) has doubled.
  - These options have been removed from the system.
- Offset payload encoding back pressure system
  - If the payload begins to take more than a certain threshold amount of the maximum available, no more messages will be brought in for processing, until the space need beings to reduce back below the threshold. This is to try to prevent the situation where the payload is too large to fit at all, and must be dropped entirely.
  - See Proper offset encoding back pressure system so that offset payloads can't ever be too large [#47](https://github.com/confluentinc/parallel-consumer/issues/47)
  - Messages that have failed to process, will always be allowed to retry, in order to reduce this pressure.

### Improvements

- Default ordering mode is now `KEY` ordering (was `UNORDERED`).
  - This is a better default as it's the safest mode yet high performing mode. It maintains the partition ordering characteristic that all keys are processed in log order, yet for most use cases will be close to as fast as `UNORDERED` when the key space is large enough.
- [Support BitSet encoding lengths longer than Short.MAX_VALUE #37](https://github.com/confluentinc/parallel-consumer/issues/37) - adds new serialisation formats that supports wider range of offsets - (32,767 vs 2,147,483,647) for both BitSet and run-length encoding.
- Commit modes have been renamed to make it clearer that they are periodic, not per message.
- Minor performance improvement, switching away from concurrent collections.

### Fixes

- Maximum offset payload space increased to correctly not be inversely proportional to assigned partition quantity.
- Run-length encoding now supports compacted topics, plus other bug fixes as well as fixes to Bitset encoding.

## v0.2.0.3

### Fixes

  - [Bitset overflow check (#35)](https://github.com/confluentinc/parallel-consumer/issues/35) - gracefully drop BitSet or Runlength encoding as an option if offset difference too large (short overflow)
    - A new serialisation format will be added in next version - see [Support BitSet encoding lengths longer than Short.MAX_VALUE #37](https://github.com/confluentinc/parallel-consumer/issues/37)
  - Gracefully drops encoding attempts if they can't be run
  - Fixes a bug in the offset drop if it can't fit in the offset metadata payload

## v0.2.0.2

### Fixes

  - Turns back on the [Bitset overflow check (#35)](https://github.com/confluentinc/parallel-consumer/issues/35)

## v0.2.0.1 DO NOT USE - has critical bug

### Fixes

  - Incorrectly turns off an over-flow check in [offset serialisation system (#35)](https://github.com/confluentinc/parallel-consumer/issues/35)

## v0.2.0.0

### Features

  - Choice of commit modes: Consumer Asynchronous, Synchronous and Producer Transactions
  - Producer instance is now optional
  - Using a _transactional_ Producer is now optional
  - Use the Kafka Consumer to commit `offsets` Synchronously or Asynchronously

### Improvements

  - Memory performance - garbage collect empty shards when in KEY ordering mode
  - Select tests adapted to non transactional (multiple commit modes) as well
  - Adds supervision to broker poller
  - Fixes a performance issue with the async committer not being woken up
  - Make committer thread revoke partitions and commit
  - Have onPartitionsRevoked be responsible for committing on close, instead of an explicit call to commit by controller
  - Make sure Broker Poller now drains properly, committing any waiting work

### Fixes

  - Fixes bug in commit linger, remove genesis offset (0) from testing (avoid races), add ability to request commit
  - Fixes #25 https://github.com/confluentinc/parallel-consumer/issues/25:
    - Sometimes a transaction error occurs - Cannot call send in state COMMITTING_TRANSACTION #25
  - ReentrantReadWrite lock protects non-thread safe transactional producer from incorrect multithreaded use
  - Wider lock to prevent transaction's containing produced messages that they shouldn't
  - Must start tx in MockProducer as well
  - Fixes example app tests - incorrectly testing wrong thing and MockProducer not configured to auto complete
  - Add missing revoke flow to MockConsumer wrapper
  - Add missing latch timeout check

## v0.1

### Features:

  - Have massively parallel consumption processing without running hundreds or thousands of
    - Kafka consumer clients
    - topic partitions

without operational burden or harming the clusters performance
  - Efficient individual message acknowledgement system (without local or third system state) to massively reduce message replay upon failure
  - Per `key` concurrent processing, per `partition` and unordered message processing
  - `Offsets` committed correctly, in order, of only processed messages, regardless of concurrency level or retries
  - Vert.x non-blocking library integration (HTTP currently)
  - Fair partition traversal
  - Zero~ dependencies (`Slf4j` and `Lombok`) for the core module
  - Java 8 compatibility
  - Throttle control and broker liveliness management
  - Clean draining shutdown cycle
