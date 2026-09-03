<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Metrics example - Parallel Consumer's meters, on a Prometheus endpoint

The same core setup as the core example, with Micrometer wired in and scraped end to end: options
take a `PrometheusMeterRegistry`, a tiny `HttpServer` serves `/prometheus` on port 7001, and the
test stands up a real Prometheus container that scrapes it.

## What to look at

`CoreApp.setupParallelConsumer()` is the whole point, and it is five numbered lines in the root
`README.adoc`'s metrics section:

- `meterRegistry(meterRegistry)` - hand PC the registry; without it no PC meter is published.
- `metricsTags(...)` and `pcInstanceTag(instanceId)` - the tags every PC meter carries. The instance
  tag is what keeps two PCs in one JVM apart.
- `new KafkaClientMetrics(kafkaConsumer).bindTo(meterRegistry)` - the *Kafka client's* own metrics,
  which are separate from PC's and are not published unless you ask.

Then `setupPrometheusEndpoint()` for the scrape endpoint - deliberately the smallest thing that
works, `com.sun.net.httpserver.HttpServer`, so the example is about the wiring and not about a web
framework.

## Running it

`./mvnw verify -pl :parallel-consumer-example-metrics -am` (from the repo root). **This one needs
Docker.** `CoreAppMetricsIntegrationTest` lives in an `integrationTests` package, which is how
failsafe selects it, and it starts a `PrometheusContainer` via Testcontainers, exposes host port
7001 to it, and asserts that `pc_status`, `pc_partitions_number`, `pc_incomplete_offsets_total` and
`pc_user_function_processing_time_seconds` all arrive. The scrape config it uses is
`src/test/resources/prometheus.yml`.

The meter-by-meter catalogue is in the root [`README.adoc`](../../README.adoc) under `== Metrics`,
and [`docs/features/micrometer-metrics.yaml`](../../docs/features/micrometer-metrics.yaml) owns the
capability record.

## One trap in this module's pom

`micrometer-registry-prometheus` is **pinned to 1.12.x**, because 1.13 renamed
`io.micrometer.prometheus` to `io.micrometer.prometheusmetrics` and that breaks this file. Its own
comment says so, and the bump is queued in
[`docs/inflight/deps-deferred-majors.md`](../../docs/inflight/deps-deferred-majors.md). The
test-scope `jackson-databind` pin next to it is module-local on purpose - promoting it to the root
pom breaks WireMock in `parallel-consumer-vertx`.
