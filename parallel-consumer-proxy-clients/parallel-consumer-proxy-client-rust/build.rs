// Copyright (C) 2026 Antony Stubbs and contributors

//! Generates the protobuf and gRPC bindings from the FROZEN schema at
//! `parallel-consumer-proxy-protocol/src/main/proto/parallelconsumer/proxy/v1/proxy.proto`.
//!
//! Codegen is a BUILD step here rather than committed output, which is the opposite of the Go
//! client's choice and is right for each language for the same reason: `cargo build` runs
//! `build.rs` on every consumer's machine, so generated code cannot drift from the schema, while
//! `go get` has no such step and must find the stubs already there. The regeneration check other
//! languages have to run by hand is therefore structural here - there is nothing committed to
//! diff.
//!
//! `protoc` is required, and is NOT installable from cargo. It is resolved in this order, and the
//! error names all three when none is found:
//!   1. `$PROTOC` - an explicit path, which is what CI rows and containers should set;
//!   2. `protoc` on `PATH` - a mise/asdf/system install, the ordinary developer case;
//!   3. the copy the protocol module's `protobuf-maven-plugin` downloads into the local Maven
//!      repository - so a machine with only a JDK and Maven can still build this crate.
//!
//! THE WELL-KNOWN TYPES ARE A SEPARATE PROBLEM FROM THE BINARY, and the schema imports two of them
//! (`duration.proto`, `timestamp.proto`). `protoc` normally finds them relative to its own
//! executable, which fails for exactly the two installations this repository uses: a mise shim
//! resolves to the mise launcher rather than to protoc, and the Maven-downloaded artifact is a bare
//! executable with no `include/` directory at all. So the include directory is resolved here too,
//! by [`well_known_types_include`], and passed explicitly.

use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    // .../parallel-consumer-proxy-clients/parallel-consumer-proxy-client-rust -> repo root
    let repo_root = manifest_dir
        .parent()
        .and_then(Path::parent)
        .ok_or("cannot locate the repository root above CARGO_MANIFEST_DIR")?
        .to_path_buf();
    let proto_root = repo_root.join("parallel-consumer-proxy-protocol/src/main/proto");
    let proto = proto_root.join("parallelconsumer/proxy/v1/proxy.proto");

    if !proto.exists() {
        return Err(format!(
            "the frozen schema is not at {} - this crate generates from the protocol module by \
             path and cannot be built outside the repository tree",
            proto.display()
        )
        .into());
    }

    if std::env::var_os("PROTOC").is_none() {
        if let Some(protoc) = protoc_from_maven_repository() {
            // SAFETY-equivalent note for a build script: this is a single-threaded main() before
            // any codegen runs, and it only fills in what the user did not set.
            std::env::set_var("PROTOC", protoc);
        }
    }

    let mut includes = vec![proto_root];
    if let Some(well_known) = well_known_types_include() {
        includes.push(well_known);
    }

    println!("cargo:rerun-if-changed={}", proto.display());
    println!("cargo:rerun-if-env-changed=PROTOC");
    println!("cargo:rerun-if-env-changed=PROTOC_INCLUDE");

    tonic_prost_build::configure()
        // Client only: this crate never serves the Session RPC, and generating a server trait
        // would put a surface here that no client may implement.
        .build_server(false)
        .build_client(true)
        .compile_protos(&[proto], &includes)
        .map_err(|e| {
            format!(
                "generating from the frozen schema failed - if the message names protoc, install \
                 it (mise install protoc, or a system package), set $PROTOC to an explicit path, \
                 or build the protocol module once so a copy lands in the local Maven repository \
                 (bin/build.sh -pl :parallel-consumer-proxy-protocol -am -DskipTests); if it names \
                 google/protobuf/duration.proto or timestamp.proto, point $PROTOC_INCLUDE at the \
                 include directory of a protoc distribution: {e}"
            )
        })?;

    Ok(())
}

/// The directory holding `google/protobuf/*.proto`, or `None` when nothing plausible is on this
/// machine - in which case `protoc` is left to its own resolution and the error message above
/// names the environment variable that fixes it.
fn well_known_types_include() -> Option<PathBuf> {
    let holds_them = |dir: &Path| dir.join("google/protobuf/duration.proto").is_file();

    if let Some(explicit) = std::env::var_os("PROTOC_INCLUDE").map(PathBuf::from) {
        if holds_them(&explicit) {
            return Some(explicit);
        }
    }

    // A real protoc distribution keeps them beside the binary, at ../include.
    if let Some(binary) = resolved_protoc() {
        if let Some(include) = binary.parent().and_then(Path::parent).map(|d| d.join("include")) {
            if holds_them(&include) {
                return Some(include);
            }
        }
    }

    // This repository's toolchains come from mise, whose PATH entry is a shim that resolves to the
    // mise launcher - so the ../include rule above cannot work for it, and the install tree is
    // where the include directory actually is.
    let mise_root = std::env::var_os("MISE_DATA_DIR")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/share/mise")))?;
    let mut mise_candidates: Vec<PathBuf> = std::fs::read_dir(mise_root.join("installs/protoc"))
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .map(|version| version.path().join("include"))
        .filter(|include| holds_them(include))
        .collect();
    mise_candidates.sort();
    if let Some(newest) = mise_candidates.pop() {
        return Some(newest);
    }

    ["/usr/local/include", "/usr/include"]
        .into_iter()
        .map(PathBuf::from)
        .find(|dir| holds_them(dir))
}

/// The `protoc` this build will actually run: `$PROTOC`, else the first `protoc` on `PATH`,
/// canonicalised so a symlink does not defeat the `../include` rule above.
fn resolved_protoc() -> Option<PathBuf> {
    let candidate = std::env::var_os("PROTOC").map(PathBuf::from).or_else(|| {
        std::env::split_paths(&std::env::var_os("PATH")?)
            .map(|dir| dir.join("protoc"))
            .find(|candidate| candidate.is_file())
    })?;
    std::fs::canonicalize(candidate).ok()
}

/// The `protoc` executable the protocol module's Maven build downloads, if this platform's copy is
/// there. Returns the newest match, mirroring what the Go client's generation script does with the
/// same directory - the two languages resolve the same fallback the same way.
fn protoc_from_maven_repository() -> Option<PathBuf> {
    let classifier = match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => "linux-x86_64",
        ("linux", "aarch64") => "linux-aarch_64",
        ("macos", "x86_64") => "osx-x86_64",
        ("macos", "aarch64") => "osx-aarch_64",
        _ => return None,
    };
    let repo = std::env::var_os("MAVEN_REPO_LOCAL")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".m2/repository")))?;
    let protoc_dir = repo.join("com/google/protobuf/protoc");

    let mut candidates: Vec<PathBuf> = std::fs::read_dir(&protoc_dir)
        .ok()?
        .filter_map(Result::ok)
        .map(|version| version.path())
        .filter_map(|version_dir| {
            let name = format!("protoc-{}-{classifier}.exe", version_dir.file_name()?.to_string_lossy());
            let candidate = version_dir.join(name);
            candidate.is_file().then_some(candidate)
        })
        .collect();
    candidates.sort();
    candidates.pop()
}
