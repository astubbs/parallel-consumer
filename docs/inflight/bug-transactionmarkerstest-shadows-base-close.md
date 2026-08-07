# `TransactionMarkersTest` shadows the base teardown, so its Kafka clients are never closed

Found while checking all 29 `BrokerIntegrationTest` subclasses for collisions before hoisting a shared
teardown into the base class. Not caused by that change, and not fixed by it.

## What is wrong

`BrokerIntegrationTest` declares a package-private `close()` annotated `@AfterEach`, which calls
`kcu.close()` to close `KafkaClientUtils`' own default producer, consumer and admin client.

`TransactionMarkersTest` declares its **own** package-private `void close()` in the same package. Java
overrides it - so the base method never runs for that class, and neither does `kcu.close()`. Its
default clients are left to the JVM.

The override is almost certainly accidental: nothing in that test appears to want to suppress the base
teardown, and its own `close()` does unrelated work.

## Why it has not bitten anyone yet

Testcontainers tears the broker down at JVM exit, and an unclosed client is not an assertion failure -
so the symptom is leaked sockets and threads for the life of the fork, not a red test. Under
`forkCount=1C` with many ITs per fork it is a slow leak rather than a visible one.

## What to do

Rename `TransactionMarkersTest#close()` to something scenario-specific, or have it call
`super.close()`. Renaming is safer: an accidental override that silently disables a base-class
`@AfterEach` is exactly the shape that comes back, and a distinct name cannot recur.

Worth a quick sweep at the same time for any other subclass declaring a method whose signature matches
a base `@AfterEach`/`@BeforeEach` - the collision check done here looked for `toClose`/`register`
specifically, not for teardown shadowing in general, so this may not be the only instance.

## Why the shared teardown was not folded into `close()`

Because of this. The hoisted `register(...)`/`toClose` teardown added in the transactional battle-test
branch is a separately-named `@AfterEach closeRegisteredTestClients()`, which no subclass shadows.
Folding it into `close()` would have meant it silently did not run for `TransactionMarkersTest`. The
two base `@AfterEach` methods have unspecified relative ordering, which is safe only because their
resource sets are disjoint - `kcu.close()` touches `KafkaClientUtils`' own default clients, and the
registered set is per-test clients, verifiers, PC instances and appenders.
