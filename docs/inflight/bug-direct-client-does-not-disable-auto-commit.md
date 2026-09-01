# The direct client leaves auto-commit on; the sidecar forces it off

Two transports that are meant to be interchangeable behave differently given identical
`ClientOptions`, on the very first thing an application does (astubbs#242).

- **Over gRPC it works.** The sidecar builds its own consumer and forces the setting: see
  `enable.auto.commit` in `KafkaClientFactory`, whose comment reads "forced `false` whatever the map
  says".
- **Over direct it throws.** `DirectParallelConsumerClient`'s `buildConsumer` hands the caller's
  `kafkaProperties` straight to `KafkaConsumer`. Kafka's own default is `true`, and
  parallel-consumer-core then refuses it: `ParallelConsumerException: Consumer auto commit must be disabled, as commits are
  handled by the library.`

So an application that supplies only the obvious properties - bootstrap, group, offset reset - runs
over one transport and fails to start on the other. There is no occurrence of `enable.auto.commit`
anywhere under `parallel-consumer-proxy-clients/`.

## Why no test caught it, and why no test can today

The direct transport's conformance suite injects its own consumer - `.consumer(mockConsumer)` in
`DirectSpikeConformanceTest` - so `buildConsumer` is never reached. The one code path that turns
user properties into a consumer is the one path the suite is built to bypass. A conformance case
that constructs a client from properties alone would close that gap.

## How it was found, and what it costs today

The Java reference demo, driving both transports over a real broker for the first time. The demo
sets `enable.auto.commit=false` itself so that every arm runs, and says why at the property; when
this is closed, that line becomes redundant rather than wrong.

The cost is not confined to Java. Ten client authors mirror the Java client's shape, and each will
either rediscover this or copy the workaround into their own demo.

## The decision this needs

Whether the client library should normalise the settings parallel-consumer-core requires - as the sidecar already does
- or whether supplying them is the application's job in every language. Either answer is
defensible; the two transports disagreeing is not. Whoever settles it owns `KafkaClientFactory` and
`DirectParallelConsumerClient` agreeing afterwards.
