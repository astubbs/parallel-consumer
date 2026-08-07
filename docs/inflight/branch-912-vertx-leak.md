# `bugs/912-vertx-stream-memory-leak` - done, no PR

Clears the JStream deque on close (`confluentinc#912`, a production memory leak), with
`JStreamMemoryLeakTest912` as the guard. Committed and pushed; vertx-module only, so it collides with
nothing else in flight.

**Rebase → open PR.** The cheapest open item to land.
