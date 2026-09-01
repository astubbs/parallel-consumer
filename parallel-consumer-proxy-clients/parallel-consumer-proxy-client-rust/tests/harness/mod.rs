// Copyright (C) 2026 Antony Stubbs and contributors

//! Locates the JVM-side sidecars so a Rust test can spawn one as an ordinary sidecar binary.
//!
//! **There are two, and they answer different questions.** Both are classpath invocations rather
//! than binaries - so "the sidecar binary" for a test is the JVM launcher and the classpath is an
//! argument - and everything awkward about that lives here rather than in each test.
//!
//! [`engine_less_sidecar`] runs `parallel-consumer-proxy`'s `NoEngineMain`, shipped in that
//! module's **test** jar beside `TestModeMain`. It hosts no Parallel Consumer engine: it binds,
//! announces its port, admits one connection under the transport's rules, and answers every
//! session `UNIMPLEMENTED` (astubbs/parallel-consumer#384). A test that
//! spawns it exercises the whole client-side path up to and including the handshake, and stops
//! exactly where the engine would begin.
//!
//! [`for_scenario`] runs `TestModeMain`, shipped in the proxy module's **test** jar so it can never
//! reach a client package. That one IS engine-backed, which is what lets `tests/session.rs` drive a
//! conformance scenario end to end.

#![allow(dead_code)] // each integration-test binary compiles this module and uses part of it

use std::path::{Path, PathBuf};

/// The no-engine sidecar entry point, in the proxy module's TEST jar: the production lifecycle with
/// the engine supplier swapped, so a session is answered UNIMPLEMENTED and this test has a subject.
pub const MAIN_CLASS: &str = "bz.stub.parallelconsumer.proxy.NoEngineMain";

/// The engine-backed harness entry point, in the proxy module's **test** jar.
pub const TEST_MODE_MAIN_CLASS: &str = "bz.stub.parallelconsumer.proxy.testmode.TestModeMain";

/// What the sidecar's refusal must name, so a client author does not debug their own code.
pub const NO_ENGINE_DESCRIPTION: &str = "hosts no Parallel Consumer engine";

/// The conformance suite's identities, used verbatim by every language's tests. **A scenario name
/// is also the topic name** - the harness seeds its records on the topic it is named after.
pub mod scenario {
    /// One record in, processed once, offset advances past it.
    pub const PROCESSED_RECORD_ADVANCES_OFFSET: &str = "a-processed-record-advances-the-committed-offset";
}

/// A spawnable command: an absolute binary path plus its arguments, which is exactly what the
/// client library asks for.
pub struct Sidecar {
    pub path: PathBuf,
    pub args: Vec<String>,
}

/// The command that runs the real sidecar shell.
///
/// **No arguments**, and that is the sidecar's own rule rather than this function being terse: it
/// takes none and refuses to start when given one, because everything is configured connect-time
/// over the protocol.
///
/// It **fails** rather than skips when the sidecar is not built. A test that quietly does not run
/// is not a passing test, and nothing goes red to say so; the error names the build command
/// instead.
pub fn engine_less_sidecar() -> Result<Sidecar, String> {
    let root = repo_root()?;
    Ok(Sidecar {
        path: java_binary()?,
        args: vec!["-cp".to_owned(), classpath(&root)?, MAIN_CLASS.to_owned()],
    })
}

/// The command that serves one conformance scenario in mock mode, engine-backed.
///
/// It **fails** rather than skips when the harness is not built, for the same reason
/// [`engine_less_sidecar`] does.
pub fn for_scenario(scenario: &str) -> Result<Sidecar, String> {
    let root = repo_root()?;
    Ok(Sidecar {
        path: java_binary()?,
        args: vec![
            "-cp".to_owned(),
            classpath(&root)?,
            TEST_MODE_MAIN_CLASS.to_owned(),
            "--mock".to_owned(),
            "--scenario".to_owned(),
            scenario.to_owned(),
        ],
    })
}

/// Walks up from this test's working directory to the enclosing git working tree. `.git` is a
/// FILE in a worktree and a directory in a primary clone, so this tests for existence rather than
/// for a directory.
fn repo_root() -> Result<PathBuf, String> {
    let mut dir = std::env::current_dir().map_err(|e| format!("harness: no working directory: {e}"))?;
    loop {
        if dir.join(".git").exists() {
            return Ok(dir);
        }
        if !dir.pop() {
            return Err("harness: no git working tree above the test's working directory".to_owned());
        }
    }
}

/// Resolves the JVM launcher. PATH lookup is acceptable HERE and nowhere else: this is test
/// scaffolding choosing a JVM, not a client library choosing which sidecar receives the user's
/// Kafka credentials.
fn java_binary() -> Result<PathBuf, String> {
    if let Some(explicit) = std::env::var_os("PC_PROXY_TEST_JAVA") {
        return Ok(PathBuf::from(explicit));
    }
    if let Some(home) = std::env::var_os("JAVA_HOME") {
        let candidate = PathBuf::from(home).join("bin/java");
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
        .map(|dir| dir.join("java"))
        .find(|candidate| candidate.is_file())
        .ok_or_else(|| "harness: no JVM found - set JAVA_HOME or PC_PROXY_TEST_JAVA".to_owned())
}

/// The sidecar's classpath, as Maven resolved it.
///
/// **One route, and it fails rather than guessing.** The `rust-e2e-harness` profile in this
/// module's pom writes `target/sidecar-classpath.txt` on `generate-test-resources`, which is the
/// only thing that reliably knows where the proxy module's output and its dependencies are - in a
/// reactor run they are class DIRECTORIES rather than jars, so hunting for a jar finds nothing
/// after a `test`-phase build and reports it as an unbuilt module. That is what the Go, Python and
/// TypeScript harnesses already do; this one used to hunt jars and paid for it.
fn classpath(root: &Path) -> Result<String, String> {
    let _ = root;
    let file = Path::new("target").join(CLASSPATH_FILE);
    let classpath = std::fs::read_to_string(&file)
        .map_err(|e| format!("harness: {} is missing - {HOW_TO_BUILD_IT}: {e}", file.display()))?;
    let classpath = classpath.trim().to_owned();
    if classpath.is_empty() {
        return Err(format!("harness: {} is empty - {HOW_TO_BUILD_IT}", file.display()));
    }
    Ok(classpath)
}

/// Written by the `rust-e2e-harness` profile in this module's pom.
const CLASSPATH_FILE: &str = "sidecar-classpath.txt";

const HOW_TO_BUILD_IT: &str = "run `./mvnw test -pl :parallel-consumer-proxy-client-rust -am \
     -Dpc.foreignClients` from the repository root, which is the same wiring the CI matrix row uses";
