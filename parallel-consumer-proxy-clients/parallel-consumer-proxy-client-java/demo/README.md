# The Java demo

```bash
# from anywhere in the repo - picks native or container for you
parallel-consumer-proxy-clients/parallel-consumer-proxy-client-java/demo/run.sh

# or, from this directory, the plain container path with nothing else needed
docker compose up
```

Needs Docker. A JDK is optional: with one, the demo runs natively and starts its broker in a
container; without one, the demo runs in a container too and the broker is a compose sibling. It
announces which it chose, and why, on its first line.

**The contract this keeps - and that every other language's demo keeps - is
[`parallel-consumer-proxy/demo/README.md`](../../../parallel-consumer-proxy/demo/README.md).**
Read that first. This file only records what is specific to Java.

## What is specific to Java

Java is the one language that can run every arm in a single JVM against a single broker, so its
demo carries three arms no other language's demo has or needs: `pc-core`, `java-direct` and
`java-raw-grpc`. They exist to price the client library and the wire hop separately. Everywhere
else the two contract arms - that language's own Kafka client, and that language over the sidecar -
are the whole demo.

The code lives in `../parallel-consumer-proxy-client-java-demo`, beside the client library it
exercises rather than beside the sidecar. An earlier version lived in the sidecar module and spoke
the protocol by hand; it demonstrated that the engine works and said nothing about the client,
which is the artifact users actually touch. That arm survives as `java-raw-grpc`, as a control.
