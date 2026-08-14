# The vanilla-`MockConsumer` tests now share one harness - what that collides with

Transient: delete this file once the branches below have rebased past it. The harness itself is
**not** in flight - it lands with astubbs#206. What is in flight is the conflict it creates for
every branch that forked before it.

Touching `parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/MockConsumer*Test*.java`?

- **The wiring moved.** Each scenario used to carry its own copy of the topic, the manual rebalance
  dance, the PC lifecycle, the record feed and the teardown; all of that is now in
  `MockConsumerTestBase`, and a scenario is a subclass overriding `createMockConsumer()` and
  `customiseOptions(..)`. So a branch that edits one of these files conflicts textually almost every
  time, and the resolution is almost always **keep the harness, re-apply your scenario** - not the
  reverse. Re-applying a pre-harness copy of the wiring silently un-does the extraction.
- **PR astubbs#202** moves `LongPollingMockConsumer` from test sources to main sources. The FQN does
  not change, so nothing breaks - but `MockConsumerTestBase`'s javadoc points at it, so a later
  rename would need to come here too.

Why the harness is shaped the way it is - why it does not extend
`AbstractParallelEoSStreamProcessorTestBase`, and why each scenario keeps its own Awaitility block
and timeout - is in `MockConsumerTestBase`'s class javadoc, which is where it stays. The
file-similarity verdicts for these files, and the deferred cross-module clone backlog, are in
[`docs/refactoring.md`](../refactoring.md) (*Cross-module test clones*). Neither is repeated here:
duplicating them is how the two copies drift apart.
