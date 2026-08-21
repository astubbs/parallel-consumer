# The C++ demo

```bash
# from anywhere in the repo
parallel-consumer-proxy-clients/parallel-consumer-proxy-client-cpp/demo/run.sh

# or, from this directory, the plain container path with nothing else needed
docker compose up
```

Needs Docker and nothing else - no C++ toolchain, no gRPC development packages, no JDK. Everything
is built in the image: the demo, the client library it links, and the sidecar it spawns.

**The contract this keeps - and that every other language's demo keeps - is
[`parallel-consumer-proxy/demo/README.md`](../../../parallel-consumer-proxy/demo/README.md).**
Read that first. This file only records what is specific to C++.

## The two arms

| arm | what it is |
|---|---|
| `AK core` | librdkafka, one record at a time, with a blocking sleep as the simulated work. C++'s own Kafka client - there is no other. |
| `cpp-grpc` | this application as a **foreign client**: the client library in the module above spawns the sidecar, receives records over a socket, runs the same sleep on them and reports outcomes back. On this path the application does no Kafka I/O at all. |

The arm goes through **this module's client library**, never through hand-written gRPC. The Java
seed was written the other way first and had to be rewritten: speaking the protocol by hand proves
the *engine* works and says nothing about the *client library*, which is the artifact users actually
touch. Java keeps a hand-written arm as a control because one JVM can hold every arm at once and the
pair prices the library. C++ has nothing to compare one against, so two arms is the whole demo -
which is the contract everywhere except Java.

The demo also creates the topic and pre-produces the backlog with librdkafka. That is not a hole in
the sidecar arm's claim: "the application does no Kafka I/O" is a statement about *that path*, and a
comparison needs both sides.

## What is specific to C++

### There is no native mode

The reference demo picks native or container and says which it chose. **C++ is container-only**, and
`run.sh --native` says so rather than failing as an unknown flag. The reason is the same one that
makes `bin/build-client.sh` build this language in an image: gRPC and protobuf arrive as system
*development packages* rather than as a versioned toolchain, so there is no C++ toolchain on a
developer's machine to run natively against. A demo that told a reader to install gRPC dev packages
first would not be a one-command demo.

Two consequences follow, and both are visible in the output:

- **The broker is always a compose sibling.** Java starts one with Testcontainers when no
  `--bootstrap` was supplied; C++ has no Testcontainers, and a demo container is never granted the
  host Docker socket (plan unit U35), so it could not start one anyway. Running the binary with no
  broker address prints an explanation and exits 2 rather than running something different.
- **The image carries its own toolchain.** The runtime stage is built from the same Debian trixie
  toolchain stage the binary was compiled in, so the dynamically linked demo is guaranteed the
  libraries it was linked against. That is the opposite of `../Dockerfile`, whose whole point is
  exporting *statically* linked artifacts that run off-image.

### The sidecar arrives as a launcher script

A foreign client library spawns a binary by absolute path; the sidecar is a JVM program. The image
installs a four-line launcher at `/app/sidecar/sidecar` that `exec`s the JVM - `exec` rather than a
fork, because the client holds the write end of that process's stdin and EOF there is the sidecar's
parent-death signal (KTD19). `PC_DEMO_SIDECAR` names it.

**That is an environment variable and not an eighth flag**: the contract fixes the demo's flag list
at seven, and where the sidecar binary lives is a property of the image rather than of the run.

### librdkafka's C API, not its C++ one

The C++ binding has no admin client, so creating the topic would drop to the C API anyway. One API
in one file beats two.

## Where the code is

`src/` beside this file - `demo.cpp` (the arms and the two tables), `demo_options.{h,cpp}` (the
flags, the environment and their precedence) and `demo_broker.{h,cpp}` (librdkafka: the AK core
consumer, the topic, the backlog). `CMakeLists.txt` is a project of its own rather than a target in
the module's, so the client library's build image never grows a Kafka client it does not use.

`tests/demo_options_test.cpp` runs under the module's own sixty-line harness and **inside the image
build**: a red test fails the image, which fails the demo. It covers the entry path - the
no-argument case, flags beating the environment, the fingerprint's silence about the broker address -
because that is where every failure this fan-out's demos have actually had lived.
