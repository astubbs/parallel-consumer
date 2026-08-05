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

`CommitRejectionTestBase` and `MockConsumerTestBase` sit around 70% on the check. That is two small
abstract harnesses in one package sharing imports, not duplication to chase - see the verdict table
in [`docs/refactoring.md`](../refactoring.md) (*Cross-module test clones*), which also ranks the
genuinely-deferred pairs so the next PR does not re-derive the audit.
