# Dependency upgrades deliberately held back

Everything is at its newest **non-major** version (`versions-maven-plugin -DallowMajorUpdates=false`
plus `bin/deps-version-rules.xml`, which also filters pre-releases and Confluent `-ce`/`-ccs` Kafka
builds - without that filter Kafka "latest" mis-resolves to a Confluent build).

| Held at | Latest | Why, and what unblocks it |
|---------|--------|---------------------------|
| kafka-clients / streams `3.9.2` | `4.3.1` | Needs the Java 11 baseline - see `pr-53-java-baseline-kafka4.md` |
| junit-jupiter / platform `5.14.4` | `6.1.2` | Needs Java 17, **and** `archunit-junit5` will not run on JUnit 6 with no `archunit-junit6` engine yet ([TNG/ArchUnit#1556](https://github.com/TNG/ArchUnit/issues/1556)). Rewire the ArchUnit tests before astubbs#38 can land |
| testcontainers `1.21.4` | `2.0.5` | Testcontainers 2.x, core artifact only (the `kafka`/`postgresql`/`junit-jupiter` modules already moved) |
| vertx-junit5 / web-client `4.5.31` | `5.1.5` | Vert.x 5 |
| mutiny `2.9.5` | `3.3.0` | Mutiny 3 |
| wiremock-jre8 `2.35.2` | `3.0.1` | WireMock 3 (artifact renamed `org.wiremock:wiremock`, test-only). **Side effect while on 2.x:** it drags in byte-buddy `1.12.18`, which wins the conflict and lacks the `JAVA_V21` field mockito 5.23 needs, so every Mockito test errors. Worked around by pinning `byte-buddy.version=1.17.7`; **remove that pin when wiremock moves to 3.x** |
| micrometer-core `1.13.15` + registry-prometheus `1.12.2` | `1.17.x` | Not a major, but source-incompatible: micrometer 1.13 renamed `io.micrometer.prometheus` → `io.micrometer.prometheusmetrics`, breaking `example-metrics/CoreApp.java`. Migrate the imports and registry construction, then bump the family together. **This migration is also the only route to a CVE fix**: the 1.13.x/1.14.x patches exist only in the commercial repo, so Maven Central's minimum fixed version is `1.15.12` - see `deps-cve-backlog.md` |
| jackson-databind `2.17.2` (example-metrics, test scope) | `2.18.9` | Module-local **on purpose**: pinning it globally forces WireMock in `parallel-consumer-vertx` onto an incompatible Jackson and breaks `VertxTest` (HTTP 500). Dependabot told to ignore (astubbs#76); bump in the next curated sweep with an example-metrics integration-test run. A *second*, separately-versioned copy reaches `example-streams` at runtime scope via `kafka-streams`; that one is already on `2.18.9` via a module-local `jackson-bom` import, and **2.18.9 is the floor** - CVE-2026-59889 was introduced in 2.18.0 and fixed only in 2.18.9, so do not land this module anywhere in 2.18.0-2.18.8. See `deps-cve-backlog.md` |
| maven-clean/deploy/install/jar/resources/source/compiler, surefire/failsafe, site | Maven-4 betas/milestones | Only pre-releases available; held by the risk policy. Revisit at GA |
