# Issue index - a discovery aid, NOT a source of truth

<!-- inflight-type: register -->
<!-- inflight-impact: blind-spot -->
<!-- issue-refs: exempt-file - every row IS a bare fork issue number; that is what the file is. The
     header states the numbers are this fork's, which is the qualification the gate wants, once. -->

**Data read from GitHub on 2026-08-26.** Regenerate with `bin/issue-index.sh`.

**Every row here goes stale silently.** An issue can be closed, retitled or relabelled the minute
after this file is written, and nothing in the repository will notice. So: use this to FIND issues,
never to decide anything about one. `gh issue view <n> -R astubbs/parallel-consumer` before you act.

## Why this exists, and why it does not break "never write down what a command can answer"

It is here for **discovery**, not storage. That rule exists to stop a second tracker forming - a
copy that gets believed and drifts apart from the truth. This one is not believed: it is dated, it
says so twice, and it sends you elsewhere before acting.

What it buys is reach. `gh issue list --state all` is the most-skipped of the six prior-art checks
in [`AGENTS.md`](../../AGENTS.md), because an agent has to think of querying GitHub before it can
do it. Grep is what agents do anyway - so a keyword sweep of `docs/` now surfaces the tracker
too. The same argument already justifies the `docs/solutions/` title index that
`.claude/hooks/inject-recorded-knowledge.sh` injects at session start.

<!-- post-merge: checked-begin -->
**The inverse failure is real too, and nothing above anticipated it: going to `gh` INSTEAD of this
file.** That reads like the more rigorous move - the live tracker over a dated copy - and the
checklist's "confirm with `gh issue view` before acting" encourages it. But the two answer different
questions: this file is for FINDING, `gh issue view` is for confirming one row before you act on it.
Reach for `gh` to do the finding and the natural query is `--json number,state,title`, which is this
file **minus the column that does the work**. A title-keyword sweep misses an issue whose title
contains none of your words; its labels do not. So if you go to `gh` for discovery, fetch
`--json labels` as well, and search `area/*` - or just grep here first, which is cheaper and was the
point. Observed on astubbs/parallel-consumer#267, where a keyword sweep of the live tracker missed
astubbs#177 (`area/reliability`, and tagged for the release the search was for) that one grep of this
file would have surfaced.
<!-- post-merge: checked-end -->

**Numbers here are this fork's.** Upstream's range overlaps ours, so a bare number is ambiguous
everywhere else - see [`docs/issue-references.md`](../issue-references.md). A row whose title
begins `confluentinc#NN:` is a mirror of that upstream issue; read the upstream original rather
than the mirror's summary.

| # | State | Title | Labels |
|---|---|---|---|
| #39 | CLOSED | PIT baseline timeout: SASL + PCMetrics tests incompatible |  |
| #40 | CLOSED | Reduce duplication in MockConsumer* test classes | 0.6.0.0 |
| #41 | OPEN | Run PIT mutation testing on self-hosted performance runner |  |
| #44 | OPEN | confluentinc#803: Transactional Producer instance gets timeout getting commit lock while second instance starts | bug, 0.6.0.0, upstream-mirror, area/reliability |
| #52 | OPEN | When will the first stable fork release be published to Maven Central? | 0.6.0.0 |
| #117 | OPEN | confluentinc#233: Refactor OffsetMapCodecManager.java | upstream-mirror, area/internals, chore |
| #118 | CLOSED | confluentinc#326: "Unexpected magic" - offset metadata PC did not write crashes the consumer on assignment | bug, upstream-mirror, area/reliability, partially-fixed-in/0.5.2.6 |
| #119 | OPEN | confluentinc#857: Paused consumption across multiple consumers | bug, upstream-mirror, area/reliability, partially-fixed-in/0.5.3.3, pr-available |
| #120 | OPEN | confluentinc#859: Memory leak in PCMetrics class | bug, 0.6.0.0, upstream-mirror, area/reliability, pr-available |
| #121 | OPEN | confluentinc#894: Offset reset when frequent rebalancing | bug, 0.6.0.0, upstream-mirror, area/reliability, pr-available |
| #122 | OPEN | confluentinc#912: Memory Leak from JStreamVertxParallelStreamProcessor | bug, documentation, upstream-mirror, area/reliability, area/api, verified bug, pr-available |
| #125 | CLOSED | confluentinc#27: Micrometer metrics | feature, upstream-mirror, area/observability, fixed-in/0.5.2.6 |
| #126 | OPEN | confluentinc#71: Health-checks | feature, good first issue, 0.6.0.0, upstream-mirror, area/observability, pr-available, next-breaking-release |
| #127 | OPEN | confluentinc#78: Allow customization of the ThreadPoolExecutor | feature, upstream-mirror, area/api, next-feature-release |
| #128 | OPEN | confluentinc#103: Matrix test against multiple AK and JDK versions in GH | 0.6.0.0, upstream-mirror, chore, area/build-test |
| #129 | OPEN | confluentinc#109: Review serialisation versioning strategy with ks and core team | upstream-mirror, chore, area/docs |
| #130 | OPEN | confluentinc#115: javadoc: Clarify how to return tombstone messages from the consume / produce loops | upstream-mirror, chore, area/docs |
| #131 | OPEN | confluentinc#130: Remove static state manipulation in tests | upstream-mirror, chore, area/build-test |
| #132 | OPEN | confluentinc#162: mvn compile fails if test-jar of parallel-consumer-core was not previously installed / deployed. | bug, upstream-mirror, area/build-test |
| #133 | OPEN | confluentinc#170: Add support for JDK's CompletableFuture into the API as part of the Reactor.io API, add docs | feature, upstream-mirror, area/api, next-feature-release |
| #134 | OPEN | confluentinc#171: Spring boot example | feature, upstream-mirror, area/docs, next-feature-release |
| #135 | OPEN | confluentinc#172: Release train for 1.0 | upstream-mirror, chore, area/release-health, 1.0, next-breaking-release |
| #136 | OPEN | confluentinc#177: Investigate using Release Drafter | upstream-mirror, chore, area/release-health |
| #137 | OPEN | confluentinc#178: How to take a single message, and distribute it to several http end points in parallel, with DLQ for failure | question, upstream-mirror, area/docs, next-feature-release |
| #138 | OPEN | confluentinc#180: vertxHttpReqInfo only supports GET (Unable to call HTTP POST or PUT etc) | help wanted, question, upstream-mirror, area/modules, next-feature-release |
| #139 | OPEN | confluentinc#186: Ensure all PC API's are thread safe | bug, upstream-mirror, area/api, blocker, 1.0, pr-available, next-breaking-release |
| #140 | CLOSED | confluentinc#192: Feature Request: Unique thread names for PC instances for logging | feature, upstream-mirror, area/observability, fixed-in/0.5.2.6 |
| #141 | OPEN | confluentinc#196: Provide option for max retires, and a call back when reached (potential DLQ) | feature, upstream-mirror, area/error-handling, pr-available, next-feature-release |
| #142 | OPEN | confluentinc#200: Refactor: Consider a shared nothing architecture, to reduce thread complexity | upstream-mirror, area/internals, chore, pr-available, next-breaking-release |
| #143 | OPEN | confluentinc#241: Try refactoring WC type from String to Enum | upstream-mirror, area/internals, chore, next-feature-release |
| #144 | OPEN | confluentinc#259: Consider adopting error prone and checker | upstream-mirror, chore, area/build-test |
| #145 | OPEN | confluentinc#266: feature: Option for batch to only contain messages of the same key | feature, good first issue, upstream-mirror, area/batching-ordering, next-feature-release |
| #146 | OPEN | confluentinc#290: Refactor test base | upstream-mirror, chore, area/build-test |
| #147 | OPEN | confluentinc#299: POC for project Loom integration for light weight threads (higher concurrency without Vert.x or Reactor) | feature, upstream-mirror, area/compat, pr-available, next-breaking-release |
| #148 | OPEN | confluentinc#304: Handle deserialization exceptions thrown from the deserialiser | feature, upstream-mirror, area/error-handling, next-feature-release |
| #149 | OPEN | confluentinc#310: Add a dead letter queue (DQL) implementation | feature, upstream-mirror, area/error-handling, pr-available, next-feature-release |
| #150 | OPEN | confluentinc#314: With KEY ordering, option to combine queues from different partitions or topics | feature, 0.6.0.0, upstream-mirror, area/batching-ordering, pr-available |
| #151 | OPEN | confluentinc#321: Transparent large message chunking and reconstruction | feature, upstream-mirror, area/batching-ordering |
| #152 | OPEN | confluentinc#322: Disk backed Produce queue | feature, upstream-mirror, area/performance |
| #153 | OPEN | confluentinc#391: Serialization error handling / flexibility | feature, upstream-mirror, area/error-handling, next-breaking-release |
| #154 | OPEN | confluentinc#394: feature: Option to have Producer send records to least loaded broker | feature, upstream-mirror, area/performance, pr-available |
| #155 | OPEN | confluentinc#402: Max loading factor steps reached: 100/100 | bug, 0.6.0.0, upstream-mirror, area/reliability |
| #156 | OPEN | confluentinc#480: Question: Is it possible to produce events using reactor?  | question, upstream-mirror, area/modules, not-a-bug, next-feature-release |
| #157 | OPEN | confluentinc#484: Question: Does Parallel-consumer have state that we can read from? | question, upstream-mirror, area/modules, next-breaking-release |
| #158 | OPEN | confluentinc#520: major: Safe User API exposure of ALL Consumer APIs (seek, end offsets etc) | feature, upstream-mirror, area/api, pr-available, next-breaking-release |
| #159 | OPEN | confluentinc#526: Move LongPollingMockConsumer to main artefact | 0.6.0.0, upstream-mirror, chore, area/build-test |
| #160 | OPEN | confluentinc#540: Apply backpressure per partition instead of the entire assignment  | feature, upstream-mirror, area/performance, next-breaking-release |
| #161 | OPEN | confluentinc#543: Why use of Scheduler if the processing has been proven NON blocking please? | question, 0.6.0.0, upstream-mirror, area/modules |
| #162 | OPEN | confluentinc#546: Truncating state | bug, upstream-mirror, area/reliability, affects/0.5.2.4, partially-fixed-in/0.5.2.6 |
| #163 | OPEN | confluentinc#550: Is There Any Exception Handler? | question, upstream-mirror, area/error-handling, next-breaking-release |
| #164 | OPEN | confluentinc#551: Batching not working as expected | bug, 0.6.0.0, upstream-mirror, area/batching-ordering |
| #165 | OPEN | confluentinc#560: Feature suggestion: Minimum batch size + batch max wait time | feature, upstream-mirror, area/batching-ordering, next-feature-release |
| #166 | CLOSED | confluentinc#597: When parallel consumer does not close kafka consumer if commmit fails during close | bug, upstream-mirror, area/reliability, fixed-in/0.5.3.1 |
| #167 | CLOSED | confluentinc#622: Wrong multiplier value in retry delay function example | bug, good first issue, 0.6.0.0, upstream-mirror, area/docs, pr-available |
| #168 | OPEN | confluentinc#629: Missing topic and offset infos when login error in ConsumerOffsetCommitter | feature, good first issue, 0.6.0.0, upstream-mirror, area/observability |
| #169 | OPEN | confluentinc#631: Warning log to verbose in RemovedPartitionState | feature, good first issue, 0.6.0.0, upstream-mirror, area/observability |
| #170 | OPEN | confluentinc#640: Error log to verbose in AbstractParallelEoSStreamProcessor | feature, good first issue, 0.6.0.0, upstream-mirror, area/observability |
| #171 | CLOSED | confluentinc#642: Add explanation of close modes to documentation | 0.6.0.0, upstream-mirror, chore, area/docs |
| #172 | OPEN | confluentinc#718: Missing feature to terminate processing | feature, upstream-mirror, area/error-handling, next-breaking-release |
| #173 | OPEN | confluentinc#777: Handling Partition Revocation in Parallel-Consumer Leading to Duplicate Event Processing | bug, upstream-mirror, area/reliability |
| #174 | OPEN | confluentinc#782: Seeking to a specific offset for a partition | feature, upstream-mirror, area/api, next-breaking-release |
| #175 | OPEN | confluentinc#809: Sporadic timeouts from ConsumerOffsetCommitter.CommitRequest | bug, upstream-mirror, area/reliability, partially-fixed-in/0.5.3.1 |
| #176 | CLOSED | confluentinc#825: `checkAutoCommitIsDisabled` fails with kafka-clients < 3.7.0 when using consumer inherited from `KafkaConsumer` | bug, upstream-mirror, area/compat, fixed-in/0.5.3.0 |
| #177 | OPEN | confluentinc#833: ParallelConsumer would run for a while and then exit due to InternalRuntimeException(Timeout) | bug, 0.6.0.0, upstream-mirror, area/reliability, affects/0.5.3.1 |
| #178 | OPEN | confluentinc#843: Record being picked up by multiple threads simultaneously | bug, upstream-mirror, area/reliability, wait for info, affects/0.5.3.0 |
| #179 | OPEN | confluentinc#860: Accept instance params for `managedExecutorService` and `managedThreadFactory` as an alternative to JNDI implementation | feature, upstream-mirror, area/api |
| #180 | OPEN | confluentinc#861: Error running tests: io.confluent.parallelconsumer.ManagedTruth.assertThat not found | bug, 0.6.0.0, upstream-mirror, area/build-test |
| #181 | OPEN | confluentinc#862: Parallel Consumer cannot run on Java 24 | bug, 0.6.0.0, upstream-mirror, area/compat |
| #182 | CLOSED | confluentinc#874: Apache-client 3.9.1 (used in spring 3.5.0) not compatible with 0.5.3.2 of parallelConsumer | bug, 0.6.0.0, upstream-mirror, area/compat, affects/0.5.3.2, fixed-in/0.5.3.3 |
| #183 | OPEN | confluentinc#875: Missing message in consumption and eventually pauses all consmption | bug, upstream-mirror, area/reliability, affects/0.5.3.1 |
| #184 | CLOSED | confluentinc#878: Migration from java 8 to 21, from springboot 2.7 to 3.5 | question, 0.6.0.0, upstream-mirror, area/compat, affects/0.5.3.2, fixed-in/0.5.3.3 |
| #185 | OPEN | confluentinc#879: Introduce no commit option | feature, upstream-mirror, area/api |
| #186 | OPEN | confluentinc#880: Please release new version after important security updates | question, 0.6.0.0, upstream-mirror, area/release-health |
| #187 | OPEN | confluentinc#884: Parallel Consumer is 30 times slower than Normal Consumer | bug, 0.6.0.0, upstream-mirror, area/performance |
| #188 | CLOSED | confluentinc#885: How to get the latest version 0.5.3.3 of 'parallel-consumer-core' ? | question, 0.6.0.0, upstream-mirror, area/release-health, fixed-in/0.5.3.3 |
| #189 | OPEN | confluentinc#887: Parellel-consumer does not behave expectedly when  a bad record cause failure for the whole batch | question, upstream-mirror, area/error-handling, next-feature-release |
| #190 | OPEN | confluentinc#896: Add support for virtual threads | feature, upstream-mirror, area/compat, pr-available, next-breaking-release |
| #191 | OPEN | confluentinc#902: Always process the freshest record when ordered by key | feature, upstream-mirror, area/batching-ordering, next-feature-release |
| #192 | OPEN | confluentinc#903: Runlength and bitarray implementation | feature, upstream-mirror, area/internals, next-feature-release |
| #193 | OPEN | confluentinc#904: support for Kafka 4.0.x | question, upstream-mirror, area/compat, next-breaking-release |
| #194 | CLOSED | confluentinc#906: Mismatch between release.version in pom.xml and dependencies | bug, 0.6.0.0, upstream-mirror, area/release-health |
| #195 | OPEN | confluentinc#907: Is the project still actively maintained? | question, 0.6.0.0, upstream-mirror, area/release-health |
| #197 | OPEN | Release 0.6.0.0 - the fork's first release | 0.6.0.0, chore, area/release-health |
| #208 | OPEN | Publish the docs as a versioned documentation site, not one 1578-line README | documentation, feature, help wanted, good first issue, chore, area/docs, next-feature-release |
| #209 | CLOSED | chaos: poll control thread submits to an already-terminated executor during cooperative revoke | bug, 0.6.0.0, area/reliability |
| #212 | OPEN | CI: our three forked actions are pinned to unreleased commits, not releases | chore, area/build-test, next-patch-release |
| #215 | OPEN | Web GUI: show what a running Parallel Consumer instance is actually doing | feature, 0.6.0.0, area/observability, pr-available |
| #216 | OPEN | Metrics: expose the buffers that have no upper bound (starting with the JStream result backlog) | feature, area/observability |
| #222 | OPEN | Metrics: expose what PC delivers - head-of-line blocking avoided, end-to-end record latency, per-shard queue depth | feature, area/observability |
| #225 | OPEN | Producer fencing kills the instance; it should abort and rejoin, like Kafka Streams' TaskMigratedException | feature, area/reliability |
| #227 | OPEN | confluentinc#21: Dynamic concurrency control with flow control or tcp congestion control theory | feature, upstream-mirror, area/performance, pr-available, next-feature-release, upstream-admin-closed |
| #228 | OPEN | confluentinc#24: Add distributed rate limiting support | feature, upstream-mirror, area/api, upstream-admin-closed |
| #229 | OPEN | confluentinc#28: Opentracing support | feature, upstream-mirror, area/observability, upstream-admin-closed |
| #230 | OPEN | confluentinc#29: performance: Support asynchronous sending of result messages | feature, upstream-mirror, area/performance, pr-available, upstream-admin-closed |
| #231 | OPEN | confluentinc#34: Monitor for progress and optionally shutdown (leave consumer group), skip message or send to DLQ | feature, upstream-mirror, area/error-handling, upstream-admin-closed |
| #232 | OPEN | confluentinc#40: Feature: Transactional mode for individual consume producer loop instead of periodic batch | feature, upstream-mirror, area/api, upstream-admin-closed |
| #233 | CLOSED | confluentinc#41: Performance: FindCompletedEligibleOffsetsAndRemove method try replacing with cache of incomplete offsets | upstream-mirror, chore, area/performance, upstream-admin-closed |
| #234 | OPEN | confluentinc#48: Support scheduled message processing (scheduled retry) | feature, upstream-mirror, area/error-handling, upstream-admin-closed |
| #235 | OPEN | confluentinc#49: Feature: Run mode where transition to plain offset committing (no encoded offsets needed) | feature, upstream-mirror, area/internals, upstream-admin-closed |
| #236 | OPEN | confluentinc#50: Feature: When subscribing to multiple topics, have the ability to priorities topics over others | feature, upstream-mirror, area/batching-ordering, pr-available, upstream-admin-closed |
| #237 | OPEN | confluentinc#53: Exact continuous offset encoding for precise offset payload size back pressure | feature, upstream-mirror, area/internals, pr-available, upstream-admin-closed |
| #238 | OPEN | confluentinc#57: Reduce debug log output | upstream-mirror, chore, area/observability, upstream-admin-closed |
| #239 | OPEN | confluentinc#65: Enhanced retry epic | feature, upstream-mirror, area/error-handling, pr-available, upstream-admin-closed |
| #240 | OPEN | confluentinc#119: Integration with Kafka Connect | feature, 0.6.0.0, upstream-mirror, area/modules, pr-available, upstream-admin-closed |
| #241 | OPEN | confluentinc#144: ProducerManager should handle different types of transaction failures appropriately | bug, upstream-mirror, area/reliability, upstream-admin-closed |
| #242 | OPEN | confluentinc#154: Integrate into a Proxy so can support any language and a server side queue implementation | feature, upstream-mirror, area/modules, upstream-admin-closed |
| #243 | OPEN | confluentinc#175: Ability use different consume and produce types for the key and value | feature, upstream-mirror, area/api, next-breaking-release, upstream-admin-closed |
| #244 | OPEN | confluentinc#183: Feature: Allow unordered processing of messages without a key in 'KEY' ordering mode | bug, upstream-mirror, area/batching-ordering, upstream-admin-closed |
| #245 | OPEN | confluentinc#187: Enable changing of topic subscription before or after PC has started | feature, upstream-mirror, area/api, pr-available, upstream-admin-closed |
| #246 | OPEN | confluentinc#191: Need ability to seek to the beginning of a partition | feature, good first issue, upstream-mirror, area/api, pr-available, upstream-admin-closed |
| #247 | OPEN | confluentinc#199: Dynamic reason support for failfast in awaitility | upstream-mirror, chore, area/build-test, upstream-admin-closed |
| #248 | OPEN | confluentinc#203: Bug: ConsumerOffsetCommitter goes into failure state after broker downtime | bug, upstream-mirror, area/reliability, wait for info, upstream-admin-closed |
| #249 | OPEN | confluentinc#205: vert.x: Run user funtions in a Vertical instead of Java thread pool | feature, upstream-mirror, area/modules, pr-available, upstream-admin-closed |
| #250 | OPEN | confluentinc#246: Consider turning on code sorting in the formatter | upstream-mirror, chore, area/build-test, not-a-bug, upstream-admin-closed |
| #251 | OPEN | confluentinc#267: docs: Comparison to Spring Cloud Stream Kafka binder | documentation, help wanted, upstream-mirror, area/docs, upstream-admin-closed |
| #252 | CLOSED | confluentinc#319: Error during shutdown - ConcurrentModificationException | bug, upstream-mirror, area/reliability, upstream-admin-closed |
| #253 | OPEN | confluentinc#320: Extend parallel consumer to be able to include multiple consumers/producers of different clusters | feature, upstream-mirror, area/modules, upstream-admin-closed |
| #254 | OPEN | confluentinc#372: Have processing functions attached to topics instead of a global one for all subscribed topics | feature, upstream-mirror, area/api, pr-available, upstream-admin-closed |
| #255 | OPEN | Kafka Streams: give a Streams topology PC's per-key parallelism | feature, 0.6.0.0, area/internals, area/performance, pr-available |
| #300 | OPEN | Upstream closure follow-up: the decisions nobody has made yet |  |
| #311 | OPEN | Batching requests a full extra in-flight target of work, and batchSize is unvalidated | bug, 0.6.0.0, area/batching-ordering, verified bug |
| #317 | OPEN | feat(core): a commit-failure seam, so the application decides instead of PC always terminating | feature |
