# Python proof-of-concept — the U26 specification probe, preserved

A working Python client, written during the specification probe (U26) by a fresh-context
agent from `parallel-consumer-proxy/docs/protocol-specification.md` and
`client-authoring-guide.md` ALONE — no Java source was read. It drove a real gRPC session
against the test-mode sidecar (`TestModeMain --mock`) and exercised: connect + configure,
wave dispatch, out-of-order per-record reports, failure → redelivery with attempt/history,
epoch fencing (stale and fabricated tokens discarded), heartbeats, terminal reports,
shutdown both directions, and byte-for-byte parsing of both golden fixtures.

**This is NOT the flagship Python client** (that unit builds the real one, with the
process-pool admin, packaging and the idiomatic API). It is preserved evidence that the
frozen protocol is implementable from the documents alone — the falsification premise
holding on its first contact with a foreign language — and seed material for the flagship.

Stubs are generated from the proto by path (see the guide); `parse_golden.py` checks the
golden fixtures; `probe.py` is the main session driver; the `probe_*.py` variants each
exercise one seam.
