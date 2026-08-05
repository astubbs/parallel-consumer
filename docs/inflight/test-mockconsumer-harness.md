# The vanilla-`MockConsumer` test harness

Cross-branch context for anyone touching
`parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/MockConsumer*Test*.java`.

## There is now one harness, and it is not the main test base

`MockConsumerTestBase` owns the wiring for every test that drives PC with a plain vanilla
`MockConsumer`: the topic, the manual rebalance dance, the PC lifecycle, the record feed (immediate
and background-daemon), and teardown. A new failure scenario is a subclass overriding
`createMockConsumer()` and `customiseOptions(..)` - **do not** start a sixth copy of the wiring.

It deliberately does **not** extend `AbstractParallelEoSStreamProcessorTestBase`, and a future
session should not "fix" that: the main base wires a Mockito-spied `LongPollingMockConsumer`, and the
whole subject of these tests is what PC does when the consumer misbehaves in ways only a hand-written
`MockConsumer` subclass can express.

Assertions stay in the subclasses, with their own Awaitility timeouts, because each timeout has to
clear that scenario's simulated outage window. Do not hoist them.

## Collides with

- **PR #202** moves `LongPollingMockConsumer` from test sources to main sources. The FQN does not
  change, so nothing here breaks - but the harness javadoc points at it, so a future rename does.
- Anything else editing these six files will conflict textually; the resolution is almost always
  "keep the harness, re-apply the scenario".

## Reading a file-similarity comment about these files

Measured on PR #206, after the extraction:

- The `MockConsumer*Test` scenarios now pair at **34-37%** (`CommitTimeout`↔`EarlyClose` 37.5%,
  `CommitTimeout`↔`Sasl` 37.2%, `EarlyClose`↔`Sasl` 34.5%), down from the 70.7% that #34 flagged and
  that motivated #40. What is left at that level is the copyright header, the import block and the
  anonymous-`MockConsumer` shape - not wiring.
- `CommitRejectionTestBase` and `MockConsumerTestBase` **do not appear in the report at all**. The
  harness extraction was predicted to push them to ~70%; it did not, and neither file reaches the
  check's 30% reporting floor against anything. Do not re-introduce that prediction.

PMD CPD reports no new clones. jscpd reports one 8-line "clone" between `MockConsumerEarlyCloseTest`
and `MockConsumerSaslAuthenticationTest` at line ~8 - that is the package declaration, copyright
header and import list, every line of which is used by both. It is not actionable in Java; overall
duplication fell 0.42% on both engines.

See the verdict table in [`docs/refactoring.md`](../refactoring.md) (*Cross-module test clones*),
which ranks the genuinely-deferred pairs so the next PR does not re-derive the audit.
