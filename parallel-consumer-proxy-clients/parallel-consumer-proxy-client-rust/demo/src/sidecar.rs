// Copyright (C) 2026 Antony Stubbs and contributors

//! Where the sidecar binary is, for the arm that reaches the engine through it.
//!
//! # The sidecar is a JVM, and that is the honest shape rather than an accident
//!
//! The proxy ships as a Java program, so "the sidecar binary" is the JVM launcher and the proxy's
//! classpath is an argument to it. A Rust application never learns that: it hands the client
//! library an absolute path and a list of arguments, and the library spawns and supervises the
//! child. Everything awkward about a classpath lives here rather than anywhere near the arm.
//!
//! **Nothing here is the client library's business, and nothing here is the demo's protocol.** The
//! demo does not speak gRPC, does not open a socket, and does not know the sidecar's port - it is
//! the client library that spawns the child, reads its port line, connects and hands records over.

use std::path::{Path, PathBuf};

/// The proxy's entry point, as `SidecarProcess` in the Java integration tests spawns it.
const MAIN_CLASS: &str = "bz.stub.parallelconsumer.proxy.Main";

/// The classpath the sidecar needs, when the caller already knows it. `demo/run.sh` and the demo's
/// Dockerfile both set this, so neither has to agree with the derivation below by coincidence.
const CLASSPATH_ENV: &str = "PC_DEMO_PROXY_CLASSPATH";

/// The JVM launcher, when the caller wants a specific one.
const JAVA_ENV: &str = "PC_DEMO_JAVA";

/// Where `run.sh` and the Dockerfile leave the classpath Maven computed.
const CLASSPATH_FILE: &str = "parallel-consumer-proxy/target/demo-proxy-classpath.txt";

/// The proxy's own compiled classes, which the dependency classpath above does not include.
const PROXY_CLASSES: &str = "parallel-consumer-proxy/target/classes";

/// A spawnable sidecar: an absolute binary path plus its arguments, which is exactly what
/// `ClientOptions` asks for.
pub struct SidecarCommand {
    /// The absolute path of the binary to spawn.
    pub path: PathBuf,
    /// Its arguments, verbatim. They carry no proxy configuration - the whole of the session's
    /// configuration travels in `Configure`, over the protocol (R39).
    pub args: Vec<String>,
}

/// Resolves the sidecar the demo will spawn.
///
/// It **fails, naming the build command**, rather than skipping the arm. A demo that quietly ran
/// one arm and reported a table with one row would look like a result.
///
/// # Errors
///
/// If no JVM can be found, or the proxy has not been built.
pub fn resolve() -> Result<SidecarCommand, String> {
    Ok(SidecarCommand {
        path: java_binary()?,
        args: vec!["-cp".to_owned(), classpath()?, MAIN_CLASS.to_owned()],
    })
}

/// The JVM launcher.
///
/// A `PATH` lookup is acceptable *here* and would not be inside the client library: this is a demo
/// choosing a JVM to run a binary out of its own build tree, not a library choosing which process
/// receives a user's Kafka credentials. The library still refuses anything but an absolute path,
/// which is why this resolves one rather than handing over the word `java`.
fn java_binary() -> Result<PathBuf, String> {
    if let Some(explicit) = std::env::var_os(JAVA_ENV) {
        let path = PathBuf::from(explicit);
        if !path.is_absolute() {
            return Err(format!("{JAVA_ENV} must be an absolute path, got {}", path.display()));
        }
        return Ok(path);
    }
    if let Some(home) = std::env::var_os("JAVA_HOME") {
        let candidate = PathBuf::from(home).join("bin/java");
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
        .map(|directory| directory.join("java"))
        .find(|candidate| candidate.is_file())
        .ok_or_else(|| {
            format!("no JVM found - the sidecar is a Java program. Set JAVA_HOME or {JAVA_ENV}.")
        })
}

/// The sidecar's classpath: what the caller supplied, or what the repository's build tree holds.
fn classpath() -> Result<String, String> {
    if let Ok(supplied) = std::env::var(CLASSPATH_ENV) {
        if !supplied.trim().is_empty() {
            return Ok(supplied);
        }
    }
    let root = repository_root(&format!("the sidecar's classpath ({CLASSPATH_FILE})"))?;
    let file = root.join(CLASSPATH_FILE);
    let dependencies = std::fs::read_to_string(&file).map_err(|e| {
        format!(
            "the sidecar's classpath is not at {} ({e}).\ndemo/run.sh builds it for you; by hand it is:\n  \
             ./mvnw --batch-mode -q -pl :parallel-consumer-proxy -am -DskipTests package \
             dependency:build-classpath '-Dmdep.outputFile=${{project.build.directory}}/demo-proxy-classpath.txt'",
            file.display()
        )
    })?;
    let classes = root.join(PROXY_CLASSES);
    if !classes.is_dir() {
        return Err(format!("the proxy is not built: {} does not exist", classes.display()));
    }
    Ok(format!("{}:{}", dependencies.trim(), classes.display()))
}

/// Walks up from the working directory to the enclosing git working tree. `.git` is a **file** in
/// a worktree and a directory in a primary clone, so this tests for existence rather than for a
/// directory - several of these demos are developed in worktrees at once.
///
/// **It cannot succeed inside the demo's own container**, because the repository-root
/// `.dockerignore` excludes `.git` from the build context. Every caller must therefore be a path
/// the container never takes, and each one says which environment variable stands in for it there.
/// The caller states what it was looking for, because this function does not know.
pub fn repository_root(wanted: &str) -> Result<PathBuf, String> {
    let mut directory =
        std::env::current_dir().map_err(|e| format!("no working directory: {e}"))?;
    loop {
        if directory.join(".git").exists() {
            return Ok(directory);
        }
        if !directory.pop() {
            return Err(format!(
                "no git working tree above this process's working directory, so {wanted} could not \
                 be located - run the demo from inside the repository, or through demo/run.sh"
            ));
        }
    }
}

/// The demo's own compose file, which the native path starts its broker from.
pub fn compose_file() -> Result<PathBuf, String> {
    if let Some(supplied) = std::env::var_os("PC_DEMO_COMPOSE_FILE") {
        return Ok(PathBuf::from(supplied));
    }
    Ok(repository_root("the demo's own compose file")?.join(demo_directory()))
}

fn demo_directory() -> &'static Path {
    Path::new("parallel-consumer-proxy-clients/parallel-consumer-proxy-client-rust/demo/docker-compose.yml")
}
