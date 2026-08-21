// Copyright (C) 2026 Antony Stubbs and contributors

package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// sidecarClasspathEnv names the file holding the sidecar's classpath.
//
// IT IS PLUMBING, NOT AN EIGHTH FLAG. The contract's environment rule is that every FLAG has a
// PC_DEMO_ variable; this one has no flag and no default a user would set, because it exists only
// because the shipped sidecar is not yet a binary. run.sh writes the file and sets this; the
// Dockerfile bakes both in. demo/README.md records it as a Go divergence for the integrator.
const sidecarClasspathEnv = "PC_DEMO_SIDECAR_CLASSPATH"

// mainClass is the sidecar's entry point: bz.stub.parallelconsumer.proxy.Main, which binds an
// ephemeral loopback port, prints "port: <n>", and serves until its parent dies.
const mainClass = "bz.stub.parallelconsumer.proxy.Main"

// sidecarCommand is what the client library spawns: an ABSOLUTE binary path plus its arguments.
//
// TODAY THAT BINARY IS THE JVM LAUNCHER AND THE PROXY IS A CLASSPATH ARGUMENT, which is a fact
// about this repository rather than about the product: the sidecar ships as a JVM application and
// has no native launcher yet. The library's contract is unaffected - it spawns one process
// directly, with no shell between it and the child, so the pipe this process holds is the one the
// sidecar watches for parent death.
type sidecarCommand struct {
	path string
	args []string
}

// resolveSidecar assembles that command, or explains what is missing.
func resolveSidecar() (sidecarCommand, error) {
	java, err := javaLauncher()
	if err != nil {
		return sidecarCommand{}, err
	}
	classpath, err := sidecarClasspath()
	if err != nil {
		return sidecarCommand{}, err
	}
	return sidecarCommand{path: java, args: []string{"-cp", classpath, mainClass}}, nil
}

// javaLauncher resolves the JVM launcher to an absolute path, preferring JAVA_HOME.
//
// A PATH LOOKUP IS ACCEPTABLE HERE AND NOWHERE IN THE LIBRARY. The library refuses a relative or
// PATH-resolved SidecarPath because it hands that binary the user's Kafka credentials; this demo is
// choosing a JVM to run a proxy it also built, against a broker it also seeded, and the same
// reasoning that lets internal/harness look java up applies. What the library receives is still
// absolute, which is the property that matters to it.
func javaLauncher() (string, error) {
	if home := os.Getenv("JAVA_HOME"); home != "" {
		candidate := filepath.Join(home, "bin", "java")
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}
	found, err := exec.LookPath("java")
	if err != nil {
		return "", fmt.Errorf("the demo found no JVM to run the sidecar with - set JAVA_HOME, or "+
			"run the demo in its container (demo/run.sh --docker): %w", err)
	}
	return filepath.Abs(found)
}

// sidecarClasspath reads the classpath file the entry point wrote.
//
// It FAILS rather than guessing. A demo that silently ran a differently-built sidecar would report
// throughput for an engine nobody asked for, and the message names the command that produces the
// file rather than leaving the reader to find it.
func sidecarClasspath() (string, error) {
	path := strings.TrimSpace(os.Getenv(sidecarClasspathEnv))
	if path == "" {
		return "", errors.New(sidecarClasspathEnv + " is not set, so the demo does not know where " +
			"the sidecar is - start the demo through demo/run.sh, which builds it and sets this")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("reading the sidecar classpath from %s: %w", path, err)
	}
	classpath := strings.TrimSpace(string(raw))
	if classpath == "" {
		return "", fmt.Errorf("the sidecar classpath at %s is empty", path)
	}
	return classpath, nil
}
