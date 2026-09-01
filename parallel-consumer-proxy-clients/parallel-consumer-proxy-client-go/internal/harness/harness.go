// Copyright (C) 2026 Antony Stubbs and contributors

// Package harness locates the JVM-side sidecars so a Go test can spawn one as an ordinary sidecar
// binary.
//
// THERE ARE TWO, AND THEY ANSWER DIFFERENT QUESTIONS. Both are classpath invocations rather than
// binaries - so "the sidecar binary" for a test is the JVM launcher and the classpath is an
// argument - and everything awkward about that lives here rather than in each test.
//
// EngineLessSidecar runs parallel-consumer-proxy's production Main, the same binary an application
// would spawn. It hosts no Parallel Consumer engine: it binds, announces its port, admits one
// connection under the transport's rules, and answers every session UNIMPLEMENTED
// (astubbs/parallel-consumer#384). A Go test that spawns it exercises the whole client-side path up
// to and including the handshake - spawn, port parse, lifeline, channel, Configure on the wire, and
// the mapping of the server's answer back to a Go error - and stops exactly where the engine would
// begin.
//
// ForScenario runs TestModeMain out of the proxy module's TEST jar, which is the engine-backed
// harness and can never reach a client package. That is what makes the conformance scenarios below
// runnable end to end rather than deferred.
//
// This package is test scaffolding that is not itself a _test.go file, because the demo and example
// waves will want it too.
package harness

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// MainClass is the no-engine sidecar entry point, in the proxy module's TEST jar. It is the
// production lifecycle - the same bind, admission rules and parent-death contract an application
// gets - with the engine supplier swapped, so a session is answered UNIMPLEMENTED and the handshake
// test has a subject. The production Main hosts the engine; spawning that here would leave nothing
// to assert the refusal against.
const MainClass = "bz.stub.parallelconsumer.proxy.NoEngineMain"

// TestModeMainClass is the engine-backed harness entry point, in the proxy module's TEST jar so it
// can never reach a client package.
const TestModeMainClass = "bz.stub.parallelconsumer.proxy.testmode.TestModeMain"

// NoEngineDescription is the substring the sidecar's refusal carries. Asserted on rather than the
// bare status code, because a description naming what is missing is what stops a client author
// debugging their own code - and asserting on it here is what keeps the two sides in step.
const NoEngineDescription = "hosts no Parallel Consumer engine"

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

// EngineLessSidecar builds the command that runs the real sidecar shell.
//
// It FAILS rather than skips when the proxy module is not built. A test that quietly does not run
// is not a passing test, and nothing goes red to say so; the error names the build command instead.
func EngineLessSidecar() (Sidecar, error) {
	java, cp, err := javaAndClasspath()
	if err != nil {
		return Sidecar{}, err
	}
	return Sidecar{
		Path: java,
		// NO ARGUMENTS, and that is the sidecar's own rule rather than this file being terse: it
		// takes none, and refuses to start when given one, because everything is configured
		// connect-time over the protocol.
		Args: []string{"-cp", cp, MainClass},
	}, nil
}

// ForScenario builds the command that serves one conformance scenario in mock mode, engine-backed.
//
// It FAILS rather than skips when the harness is not built, for the same reason EngineLessSidecar
// does.
func ForScenario(scenario string) (Sidecar, error) {
	java, cp, err := javaAndClasspath()
	if err != nil {
		return Sidecar{}, err
	}
	return Sidecar{
		Path: java,
		Args: []string{"-cp", cp, TestModeMainClass, "--mock", "--scenario", scenario},
	}, nil
}

// javaAndClasspath resolves the two things both sidecar commands need. Shared rather than repeated,
// so the two entry points cannot drift in how they locate a JVM or a classpath.
func javaAndClasspath() (string, string, error) {
	root, err := repoRoot()
	if err != nil {
		return "", "", err
	}
	java, err := javaBinary()
	if err != nil {
		return "", "", err
	}
	cp, err := classpath(root)
	if err != nil {
		return "", "", err
	}
	return java, cp, nil
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

// sidecarClasspathFile is written by the go-e2e-harness profile in this module's pom (see
// pom.xml, the `sidecar-classpath` execution). It is the WHOLE harness classpath - the engine, the
// proxy's test jar carrying TestModeMain, core's test jar and every test-scope dependency - already
// resolved by Maven, which is the only thing that reliably knows where they are.
const sidecarClasspathFile = "sidecar-classpath.txt"

// classpath assembles the harness classpath.
//
// TWO ROUTES, IN THIS ORDER, AND THE FIRST IS THE ONE CI TAKES. Under
// `./mvnw test -pl :parallel-consumer-proxy-client-go -am -Dpc.foreignClients` the profile has
// already declared the engine as a test dependency - which is what pulls it into the reactor ahead
// of this module - and written the resolved classpath to target/sidecar-classpath.txt. Reading that
// is exact, needs no jars on disk, and works before the `package` phase, where reactor artifacts are
// output DIRECTORIES rather than jars.
//
// The second route is the standalone developer running `go test ./...` by hand after building the
// proxy: hunt the jars and resolve the dependency list once, cached beside the build output because
// resolving costs seconds and the answer only changes when the proxy module's poms do. There is no
// committed classpath file either way: it is machine-specific, being absolute paths into a local
// repository.
func classpath(root string) (string, error) {
	goTarget := filepath.Join(root, "parallel-consumer-proxy-clients",
		"parallel-consumer-proxy-client-go", "target")
	if fromMaven, err := os.ReadFile(filepath.Join(goTarget, sidecarClasspathFile)); err == nil {
		return strings.TrimSpace(string(fromMaven)), nil
	}

	proxyTarget := filepath.Join(root, "parallel-consumer-proxy", "target")
	testsJar, err := singleJar(proxyTarget, "-tests.jar")
	if err != nil {
		return "", err
	}
	mainJar, err := singleJar(proxyTarget, ".jar")
	if err != nil {
		return "", err
	}

	cacheDir := goTarget
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
