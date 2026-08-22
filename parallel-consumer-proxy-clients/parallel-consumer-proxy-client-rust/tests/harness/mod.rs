// Copyright (C) 2026 Antony Stubbs and contributors

//! Locates the JVM-side conformance harness so a Rust test can spawn it as an ordinary sidecar
//! binary.
//!
//! The harness is `TestModeMain`, shipped in the proxy module's **test** jar so it can never reach
//! a client package. That makes it a classpath invocation rather than a binary, so "the sidecar
//! binary" for a conformance test is the JVM launcher and the classpath is an argument. Everything
//! awkward about that lives here rather than in each test.

#![allow(dead_code)] // each integration-test binary compiles this module and uses part of it

use std::path::{Path, PathBuf};
use std::process::Command;

/// The harness entry point.
pub const MAIN_CLASS: &str = "bz.stub.parallelconsumer.proxy.testmode.TestModeMain";

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

/// The command that serves one conformance scenario in mock mode.
///
/// It **fails** rather than skips when the harness is not built. A test that quietly does not run
/// is not a passing test, and nothing goes red to say so; the error names the build command
/// instead.
pub fn for_scenario(scenario: &str) -> Result<Sidecar, String> {
    let root = repo_root()?;
    Ok(Sidecar {
        path: java_binary()?,
        args: vec![
            "-cp".to_owned(),
            classpath(&root)?,
            MAIN_CLASS.to_owned(),
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

/// Assembles the proxy module's test classpath: its test jar (which carries the harness), its main
/// jar, and its test-scope dependencies.
///
/// The dependency list comes from Maven and is cached beside this module's build output, because
/// resolving it costs seconds and the answer only changes when the proxy module's poms do. There
/// is no committed classpath file: it is machine-specific, being a list of absolute paths into a
/// local repository.
fn classpath(root: &Path) -> Result<String, String> {
    let proxy_target = root.join("parallel-consumer-proxy/target");
    let tests_jar = single_jar(&proxy_target, "-tests.jar")?;
    let main_jar = single_jar(&proxy_target, ".jar")?;

    let cache_dir = root.join("parallel-consumer-proxy-clients/parallel-consumer-proxy-client-rust/target");
    let cache = cache_dir.join("proxy-test-classpath.txt");
    if !cache.is_file() {
        std::fs::create_dir_all(&cache_dir).map_err(|e| format!("harness: {}: {e}", cache_dir.display()))?;
        resolve_classpath(root, &cache)?;
    }
    let dependencies =
        std::fs::read_to_string(&cache).map_err(|e| format!("harness: reading {}: {e}", cache.display()))?;

    Ok([
        tests_jar.display().to_string(),
        main_jar.display().to_string(),
        dependencies.trim().to_owned(),
    ]
    .join(":"))
}

/// Asks Maven for the proxy module's test-scope dependencies, two ways.
///
/// The reactor form is tried first because it is the correct one: it resolves this repository's own
/// snapshots from the reactor rather than from a local repository that may not hold them. It reads
/// **every** module's pom, though, so an unrelated module with a malformed pom fails it - which is
/// not hypothetical on a branch several agents are editing at once. The single-project form is the
/// fallback: it reads only this module and its parents, at the cost of needing the sibling
/// snapshots installed.
fn resolve_classpath(root: &Path, cache: &Path) -> Result<(), String> {
    let common = ["-q", "dependency:build-classpath", "-Dmdep.includeScope=test"];
    let output_file = format!("-Dmdep.outputFile={}", cache.display());
    let proxy_pom = root.join("parallel-consumer-proxy/pom.xml");
    let attempts: [Vec<String>; 2] = [
        vec!["-pl".to_owned(), ":parallel-consumer-proxy".to_owned()],
        vec!["-f".to_owned(), proxy_pom.display().to_string()],
    ];

    let mut last = String::new();
    for scoping in attempts {
        let output = Command::new(root.join("mvnw"))
            .current_dir(root)
            .args(&scoping)
            .args(common)
            .arg(&output_file)
            .output()
            .map_err(|e| format!("harness: running mvnw: {e}"))?;
        if output.status.success() {
            return Ok(());
        }
        last = String::from_utf8_lossy(&output.stdout).into_owned();
    }
    Err(format!(
        "harness: resolving the proxy module's test classpath failed both ways:\n{last}"
    ))
}

fn single_jar(dir: &Path, suffix: &str) -> Result<PathBuf, String> {
    let build_first = "run 'bin/build.sh -pl :parallel-consumer-proxy -am -DskipTests' first \
         (the harness lives in the proxy module's test jar, and this module has no Maven \
         dependency on it, so -am cannot pull it in)";
    let entries =
        std::fs::read_dir(dir).map_err(|e| format!("harness: {} is not built - {build_first}: {e}", dir.display()))?;

    let mut matches: Vec<PathBuf> = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            let name = path.file_name().unwrap_or_default().to_string_lossy().into_owned();
            if !name.ends_with(suffix) {
                return false;
            }
            // -sources.jar and -javadoc.jar also end in .jar; the plain artifact is the one whose
            // remaining suffix carries no classifier.
            suffix != ".jar"
                || !(name.ends_with("-tests.jar") || name.ends_with("-sources.jar") || name.ends_with("-javadoc.jar"))
        })
        .collect();
    matches.sort();

    match matches.len() {
        1 => Ok(matches.remove(0)),
        found => Err(format!(
            "harness: expected exactly one {suffix:?} jar in {}, found {found} - {build_first}",
            dir.display()
        )),
    }
}
