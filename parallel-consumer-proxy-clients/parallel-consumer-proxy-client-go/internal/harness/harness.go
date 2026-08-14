// Copyright (C) 2026 Antony Stubbs and contributors

// Package harness locates the JVM-side conformance harness so a Go test can spawn it as an
// ordinary sidecar binary.
//
// The harness is TestModeMain, shipped in the proxy module's TEST jar so it can never reach a
// client package. That makes it a classpath invocation rather than a binary, so "the sidecar
// binary" for a conformance test is the JVM launcher and the classpath is an argument. Everything
// awkward about that lives here rather than in each test.
//
// This package is test scaffolding that is not itself a _test.go file, because the demo and
// example waves will want it too.
package harness

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// MainClass is the harness entry point.
const MainClass = "bz.stub.parallelconsumer.proxy.testmode.TestModeMain"

// Scenario names, which are the conformance suite's identities everywhere: the harness CLI, this
// list, and the Go test names that run them. A scenario name is ALSO the topic name to subscribe
// to - the harness seeds its records on the topic it is named after.
const (
	ScenarioProcessedRecordAdvancesOffset = "a-processed-record-advances-the-committed-offset"
	ScenarioUnreportedRecordHoldsCommit   = "an-unreported-record-holds-back-the-commit"
	ScenarioFailedRecordIsRedelivered     = "a-failed-record-is-redelivered-with-its-failure-history"
	ScenarioKeyOrdering                   = "records-sharing-a-key-share-a-shard-distinct-keys-run-concurrently"
)

// Sidecar is a spawnable command: an absolute binary path plus its arguments, which is exactly
// what the client library asks for.
type Sidecar struct {
	Path string
	Args []string
}

// ForScenario builds the command that serves one conformance scenario in mock mode.
//
// It FAILS rather than skips when the harness is not built. A test that quietly does not run is
// not a passing test, and nothing goes red to say so; the error names the build command instead.
func ForScenario(scenario string) (Sidecar, error) {
	root, err := repoRoot()
	if err != nil {
		return Sidecar{}, err
	}
	java, err := javaBinary()
	if err != nil {
		return Sidecar{}, err
	}
	cp, err := classpath(root)
	if err != nil {
		return Sidecar{}, err
	}
	return Sidecar{
		Path: java,
		Args: []string{"-cp", cp, MainClass, "--mock", "--scenario", scenario},
	}, nil
}

// repoRoot walks up from this package to the enclosing git working tree.
func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("harness: no git working tree above the test's working directory")
		}
		dir = parent
	}
}

// javaBinary resolves the JVM launcher. PATH lookup is acceptable HERE and nowhere else: this is
// test scaffolding choosing a JVM, not a client library choosing which sidecar receives the user's
// Kafka credentials.
func javaBinary() (string, error) {
	if explicit := os.Getenv("PC_PROXY_TEST_JAVA"); explicit != "" {
		return explicit, nil
	}
	if home := os.Getenv("JAVA_HOME"); home != "" {
		candidate := filepath.Join(home, "bin", "java")
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}
	found, err := exec.LookPath("java")
	if err != nil {
		return "", fmt.Errorf("harness: no JVM found - set JAVA_HOME or PC_PROXY_TEST_JAVA: %w", err)
	}
	return filepath.Abs(found)
}

// classpath assembles the proxy module's test classpath: its test jar (which carries the harness),
// its main jar, and its test-scope dependencies.
//
// The dependency list comes from Maven and is cached beside the build output, because resolving it
// costs seconds and the answer only changes when the proxy module's poms do. There is no committed
// classpath file: it is machine-specific, being a list of absolute paths into a local repository.
func classpath(root string) (string, error) {
	proxyTarget := filepath.Join(root, "parallel-consumer-proxy", "target")
	testsJar, err := singleJar(proxyTarget, "-tests.jar")
	if err != nil {
		return "", err
	}
	mainJar, err := singleJar(proxyTarget, ".jar")
	if err != nil {
		return "", err
	}

	cacheDir := filepath.Join(root, "parallel-consumer-proxy-clients", "parallel-consumer-proxy-client-go", "target")
	cache := filepath.Join(cacheDir, "proxy-test-classpath.txt")
	deps, err := os.ReadFile(cache)
	if err != nil {
		if err := os.MkdirAll(cacheDir, 0o755); err != nil {
			return "", err
		}
		cmd := exec.Command(filepath.Join(root, "mvnw"), "-q", "-pl", ":parallel-consumer-proxy",
			"dependency:build-classpath", "-Dmdep.outputFile="+cache, "-Dmdep.includeScope=test")
		cmd.Dir = root
		if out, runErr := cmd.CombinedOutput(); runErr != nil {
			return "", fmt.Errorf("harness: resolving the proxy module's test classpath: %w\n%s", runErr, out)
		}
		deps, err = os.ReadFile(cache)
		if err != nil {
			return "", err
		}
	}

	return strings.Join([]string{testsJar, mainJar, strings.TrimSpace(string(deps))}, string(os.PathListSeparator)), nil
}

func singleJar(dir, suffix string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", fmt.Errorf("harness: %s is not built - run "+
			"'bin/build.sh -pl :parallel-consumer-proxy -am -DskipTests' first: %w", dir, err)
	}
	var matches []string
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, suffix) {
			continue
		}
		// -sources.jar and -javadoc.jar also end in .jar; the plain artifact is the one whose
		// remaining suffix carries no classifier.
		if suffix == ".jar" && (strings.HasSuffix(name, "-tests.jar") ||
			strings.HasSuffix(name, "-sources.jar") || strings.HasSuffix(name, "-javadoc.jar")) {
			continue
		}
		matches = append(matches, filepath.Join(dir, name))
	}
	if len(matches) != 1 {
		return "", fmt.Errorf("harness: expected exactly one %q jar in %s, found %d - "+
			"run 'bin/build.sh -pl :parallel-consumer-proxy -am -DskipTests'", suffix, dir, len(matches))
	}
	return matches[0], nil
}
