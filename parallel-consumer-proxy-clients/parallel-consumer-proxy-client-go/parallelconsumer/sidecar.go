// Copyright (C) 2026 Antony Stubbs and contributors

package parallelconsumer

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// portLinePrefix is the lifecycle channel's whole vocabulary: the sidecar prints "port: <n>" and
// nothing else before it.
const portLinePrefix = "port: "

// sidecar is the proxy child process and the lifecycle pipe that keeps it alive.
//
// The pipe is the parent-death signal: this process holds the write end and never writes to it, so
// EOF on the child's stdin is proof the parent is gone. That is why the binary is launched
// DIRECTLY and never through a shell - a wrapper process would hold the write end open and leak a
// JVM that still holds group membership.
type sidecar struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	port   int

	// drain runs for the child's whole life, so a sidecar that keeps logging after the port line
	// never blocks on a full pipe buffer.
	drained sync.WaitGroup
}

func startSidecar(ctx context.Context, opts Options) (*sidecar, error) {
	// exec.Command, not a shell: see the type comment.
	cmd := exec.Command(opts.SidecarPath, opts.SidecarArgs...)
	if opts.SidecarStderr != nil {
		cmd.Stderr = opts.SidecarStderr
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("parallelconsumer: sidecar stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("parallelconsumer: sidecar stdout pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("parallelconsumer: starting sidecar %s: %w", opts.SidecarPath, err)
	}

	s := &sidecar{cmd: cmd, stdin: stdin, stdout: stdout}

	port, err := s.readPort(ctx)
	if err != nil {
		s.stop(0)
		return nil, err
	}
	s.port = port
	return s, nil
}

// readPort scans the lifecycle channel for the port line.
//
// The specification's contract is that the port is stdout's FIRST line. The conformance harness
// diverges - it logs before it - and the guide says a test absorbs that rather than asserting the
// position, so this scans for the line instead of reading exactly one. Scanning satisfies both.
func (s *sidecar) readPort(ctx context.Context) (int, error) {
	type result struct {
		port int
		err  error
	}
	done := make(chan result, 1)

	s.drained.Add(1)
	go func() {
		defer s.drained.Done()
		scanner := bufio.NewScanner(s.stdout)
		found := false
		for scanner.Scan() {
			line := scanner.Text()
			if found {
				continue // keep draining so the child never blocks on its stdout pipe
			}
			rest, ok := strings.CutPrefix(line, portLinePrefix)
			if !ok {
				continue
			}
			port, err := strconv.Atoi(strings.TrimSpace(rest))
			if err != nil {
				done <- result{err: fmt.Errorf("parallelconsumer: unparseable port line %q: %w", line, err)}
				found = true
				continue
			}
			done <- result{port: port}
			found = true
		}
		if !found {
			err := scanner.Err()
			if err == nil {
				err = io.EOF
			}
			done <- result{err: fmt.Errorf("parallelconsumer: sidecar stdout ended before a %q line: %w", portLinePrefix, err)}
		}
	}()

	select {
	case r := <-done:
		return r.port, r.err
	case <-ctx.Done():
		return 0, fmt.Errorf("parallelconsumer: waiting for the sidecar's port line: %w", ctx.Err())
	}
}

// stop closes the lifecycle pipe and reaps the child.
//
// Closing stdin is the reap: it is the parent-death signal the sidecar watches, and it is also the
// only thing that ends the conformance harness, which serves until stdin EOF and does not exit
// after a clean drain. Kill is the backstop for a child that ignores both.
func (s *sidecar) stop(grace time.Duration) error {
	if s.stdin != nil {
		_ = s.stdin.Close()
	}

	exited := make(chan error, 1)
	go func() { exited <- s.cmd.Wait() }()

	var waitErr error
	select {
	case waitErr = <-exited:
	case <-time.After(grace):
		_ = s.cmd.Process.Kill()
		waitErr = <-exited
	}

	if s.stdout != nil {
		_ = s.stdout.Close()
	}
	s.drained.Wait()

	// A killed or non-zero child is not this call's failure to report: the session already ended,
	// and the caller's error (if any) is the interesting one.
	if waitErr != nil {
		return fmt.Errorf("parallelconsumer: sidecar exited: %w", waitErr)
	}
	return nil
}
