// Copyright (C) 2026 Antony Stubbs and contributors

// Package harness locates the JVM-side sidecar so a Go test can spawn it as an ordinary sidecar
// binary.
//
// The sidecar is parallel-consumer-proxy's Main, which is a classpath invocation rather than a
// binary - so "the sidecar binary" for a test is the JVM launcher and the classpath is an argument.
// Everything awkward about that lives here rather than in each test.
//
// WHAT THIS BUILD'S SIDECAR DOES, AND WHY THE TESTS ARE SHAPED THE WAY THEY ARE. The sidecar on
// this stack hosts no Parallel Consumer engine: it binds, announces its port, admits one connection
// under the transport's rules, and answers every session UNIMPLEMENTED
// (astubbs/parallel-consumer#384). So a Go test that spawns it exercises the whole client-side path
// up to and including the handshake - spawn, port parse, lifeline, channel, Configure on the wire,
// and the mapping of the server's answer back to a Go error - and stops exactly where the engine
// would begin. Nothing here fakes the missing half; the dispatch scenarios are the shared
// conformance suite's, and they are deferred until an engine exists to run them against.
//
// On feats/proxy-requirements this same package resolves TestModeMain out of the proxy module's
// TEST jar, which is the engine-backed harness. When the engine rung lands, THAT is what this file
// points at, and the deferred scenarios come alive - the classpath plumbing below does not change.
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

// MainClass is the sidecar entry point - the production one, in the proxy module's MAIN jar. It is
// the same binary an application would spawn; nothing about this test path is a special build.
const MainClass = "bz.stub.parallelconsumer.proxy.Main"

// NoEngineDescription is the substring the sidecar's refusal carries. Asserted on rather than the
// bare status code, because a description naming what is missing is what stops a client author
// debugging their own code - and asserting on it here is what keeps the two sides in step.
const NoEngineDescription = "hosts no Parallel Consumer engine"

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
		// NO ARGUMENTS, and that is the sidecar's own rule rather than this file being terse: it
		// takes none, and refuses to start when given one, because everything is configured
		// connect-time over the protocol.
		Args: []string{"-cp", cp, MainClass},
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

// sidecarClasspathFile is written by the go-sidecar-harness profile in this module's pom (see
// pom.xml, the `sidecar-classpath` execution). It is the WHOLE sidecar classpath - the proxy module
// and every one of its dependencies - already resolved by Maven, which is the only thing that
// reliably knows where they are.
const sidecarClasspathFile = "sidecar-classpath.txt"

// classpath assembles the sidecar classpath.
//
// TWO ROUTES, IN THIS ORDER, AND THE FIRST IS THE ONE CI TAKES. Under
// `./mvnw test -pl :parallel-consumer-proxy-client-go -am -Dpc.foreignClients` the profile has
// already declared the sidecar as a test dependency - which is what pulls it into the reactor ahead
// of this module - and written the resolved classpath to target/sidecar-classpath.txt. Reading that
// is exact, needs no jars on disk, and works before the `package` phase, where reactor artifacts are
// output DIRECTORIES rather than jars.
//
// The second route is the standalone developer running `go test ./...` by hand after building the
// proxy: hunt the jar and resolve the dependency list once, cached beside the build output because
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
	mainJar, err := singleJar(proxyTarget, ".jar")
	if err != nil {
		return "", err
	}

	cacheDir := goTarget
	cache := filepath.Join(cacheDir, "proxy-runtime-classpath.txt")
	deps, err := os.ReadFile(cache)
	if err != nil {
		if err := os.MkdirAll(cacheDir, 0o755); err != nil {
			return "", err
		}
		cmd := exec.Command(filepath.Join(root, "mvnw"), "-q", "-pl", ":parallel-consumer-proxy",
			"dependency:build-classpath", "-Dmdep.outputFile="+cache, "-Dmdep.includeScope=runtime")
		cmd.Dir = root
		if out, runErr := cmd.CombinedOutput(); runErr != nil {
			return "", fmt.Errorf("harness: resolving the proxy module's classpath: %w\n%s", runErr, out)
		}
		deps, err = os.ReadFile(cache)
		if err != nil {
			return "", err
		}
	}

	return strings.Join([]string{mainJar, strings.TrimSpace(string(deps))}, string(os.PathListSeparator)), nil
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
