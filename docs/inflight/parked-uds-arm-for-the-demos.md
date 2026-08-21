# A Unix-domain-socket arm for the demos

Deferred by the owner (2026-08-21) as a feature the per-language demos should eventually carry, not
as something this PR does. Recorded here with the measurements that make restarting it cheap
(astubbs#242).

## What it would buy

The reference demo's `java-direct` to `java-grpc` step is currently a lump: it contains
serialization, the gRPC machinery, the process boundary **and** the TCP/IP stack, and the demo
attributes the whole ~2x to "going out of process". A UDS arm changes exactly one term - same gRPC,
same protobuf, same engine, same spawned sidecar, a kernel-domain socket instead of loopback TCP -
so it splits the last of those out instead of inferring it.

It would also produce a number the project does not have. KTD11 declines UDS for v1 and records, in
the same entry, that loopback TCP has **no peer-credential mechanism at all** (`SO_PEERCRED` is
UDS-only), which is the residual security risk it defers past v6. That decision currently rests on
parity and scope alone; a measured cost would let it rest on evidence too. **A demo arm does not
overturn KTD11** - it informs it.

## What is actually available, verified rather than assumed

Checked against the jar this build resolves, `grpc-netty-shaded` **1.75.0** (KTD11 says 1.73.0; the
version has moved, the conclusion has not):

- Bundled: `EpollServerDomainSocketChannel`, `EpollDomainSocketChannel`, and the epoll natives for
  **both** `x86_64` and `aarch_64`.
- Not bundled: any kqueue transport. That is the whole macOS problem.

**The container path already solves the macOS problem, which was not obvious.** Docker on macOS runs
a Linux VM, so the demo's own container is Linux on aarch64 - and the shaded jar carries that native.
A UDS arm therefore runs inside `docker compose up` on a Mac, with **no new dependency and using the
exact artifact the product ships**. Only a *native* macOS run is excluded.

So the arm's availability matrix is: container anywhere, yes; native Linux, yes; native macOS, no -
skipped and announced, the way the demo already announces which mode it chose.

## Why "unshaded netty" comes up, and why it is probably unnecessary

`grpc-netty-shaded` relocates netty into `io.grpc.netty.shaded.io.netty.*`. An external
`netty-transport-native-kqueue` supplies `io.netty.channel.kqueue.*` - the un-relocated package - so
it cannot plug into the relocated builder. Getting kqueue therefore means dropping to unshaded
`grpc-netty` with a version-matched kqueue artifact. That is a packaging consequence, not a
preference.

Given the container finding above, **that whole branch is only needed to make the arm run natively on
macOS**, which is a developer convenience rather than a product requirement. Do not reach for it
first.

## The real cost is not the socket

The demo arm is small. The sidecar-side change is small in code and larger in blast radius:

- `ProxyServer` already binds through `NettyServerBuilder.forAddress(SocketAddress)` - its javadoc
  insists on that form over `forPort` - so a `DomainSocketAddress` is the same call plus a channel
  type and an epoll event-loop group. No redesign.
- **But it moves a cleared security gate.** KTD11's mechanism is loopback bind plus a
  `ServerInterceptor` reading `ServerCall.getAuthority()`. Over a domain socket the authority is not
  what it is over TCP, so that interceptor's behaviour has to be re-proven rather than assumed - and
  the gate was cleared with counter evidence, which is the standard any replacement inherits.
- `Main` announces a port on stdout and `SidecarProcess` parses it; a socket path needs the same
  contract extended, not replaced.
- `GrpcParallelConsumerClient.builder()` takes a `port` today, so a UDS client needs either a new
  builder input or a demo-local channel.

## Where it lands

The demo contract (`parallel-consumer-proxy/demo/README.md`) says two arms are what every language
mirrors and Java's extras are diagnostics. A UDS arm is a third Java diagnostic on that list until
someone shows a second language can run one, so adding it does not widen the ten-language contract.
