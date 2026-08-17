# `close()` and `closeDrainFirst()` never release the Vert.x engine

`VertxParallelEoSStreamProcessor` overrides only `close(Duration, DrainingMode)` - the one place
`webClient.close()` and `vertx.close()` are called. Every no-argument shutdown (`close()`,
`closeDrainFirst()`, `closeDontDrainFirst()`) routes through `close(DrainingMode)` instead and never
reaches them, so the web client and the event-loop group are stranded.

Pre-existing; found while fixing astubbs#122 and deliberately left out of that PR's scope. Its own
vertx unit tests work around it in `VertxBaseUnitTest`, with an `@AfterEach` calling the
`Duration`-taking form - which is a test-side patch, not a fix.

Two things to settle when someone takes it:

- **Where the cleanup belongs.** Overriding `close(DrainingMode)` to delegate to the `Duration` form
  puts every shutdown through the same teardown. That is the small fix.
- **It is not exception-safe either.** The Vert.x teardown runs *after* `super.close(...)` with no
  `finally`, so a shutdown that throws skips it regardless of which entry point was used.

Unowned.
