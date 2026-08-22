# Build the Swift client without a container

Swift is one of only two languages that build in Docker, and the only one where that choice was
forced rather than chosen. Getting it onto the host toolchain would make its loop as fast as every
other language's, remove a 3.4 GB image from CI, and leave C++ as the sole container language.

## Why it is containerised today

`mise use -g swift@latest` **404s on this box**. Swift.org publishes per-distribution Linux
toolchains - Ubuntu, Amazon Linux, RHEL - and **Debian 13 (trixie) is not among them**, so the
version-manager route has nothing to fetch. The container was the honest way around that, and it
works: the official `swift:6.1` image, a statically linked artifact extracted with BuildKit, and a
dynamically-linked control that must fail to prove the static claim.

## Three routes worth testing, cheapest first

- **Run the Ubuntu 24.04 toolchain on Debian 13.** glibc is forward compatible, and trixie's is newer
  than Ubuntu 24.04's, so a toolchain built against the older one should run. This is a download and
  a `PATH` entry - **test it before believing either way**, because if it works the rest of this note
  is unnecessary.
- **`swiftly`**, Swift's own toolchain installer (its `rustup` equivalent). It knows about per-distro
  builds and may already handle or tolerate Debian. If it works, it is also the answer for anyone
  else's machine, which the container never was.
- **The Swift 6 static Linux SDK.** Swift 6 ships a fully static SDK targeting musl, intended for
  cross-compiling Linux binaries from any host. That is exactly the artifact shape this client already
  wants - a static binary the conformance suite executes directly - so it may fit better than the
  native toolchain does.

## What "done" looks like

`bin/build-client.sh swift` builds natively when a toolchain is present, and falls back to the
container when it is not - the script already owns that seam, and the same shape would serve any
contributor whose distribution Swift.org does not publish for. The **static/dynamic pairing must
survive**: it is what proves the artifact is portable, and it is the check that fails loudly if a
change quietly starts producing dynamically-linked output.

Keep the container path working rather than deleting it. CI runners and other machines will not all
have a toolchain, and the image is the reason this language exists here at all.

## Order

**After the Swift client works.** A build route optimised around a client that does not yet exist is
optimised for the wrong shape - the same rule that governs
[`next-container-image-cache-and-size.md`](next-container-image-cache-and-size.md), which covers the
CI caching and image size while the container route remains.
