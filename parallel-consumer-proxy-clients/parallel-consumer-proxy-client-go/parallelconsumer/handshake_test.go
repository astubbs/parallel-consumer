// Copyright (C) 2026 Antony Stubbs and contributors

package parallelconsumer_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/astubbs/parallel-consumer/parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/internal/harness"
	"github.com/astubbs/parallel-consumer/parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/parallelconsumer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestTheHandshakeReachesTheSessionServiceAndItsRefusalReachesTheCaller is this module's one
// against-a-real-process test, and the only claim it can honestly make on this stack.
//
// The sidecar it spawns is the production entry point of parallel-consumer-proxy - a real bind, the
// real authority allowlist, the real single-connection guard, and the real session service. That
// service hosts no engine and refuses every session, so there is no dispatch to observe here and
// none is invented. What IS observed is everything the Go library does before the engine would
// matter: spawn the child, read `port:` off its stdout, hold its stdin as the parent-death lifeline,
// open the channel, put Configure on the wire, and turn what came back into a Go error.
//
// THE STATUS CODE IS THE ASSERTION, NOT MERELY "IT FAILED". A refusal from the authority allowlist
// is PERMISSION_DENIED and one from the admission slot is RESOURCE_EXHAUSTED, both raised by
// interceptors BEFORE the service method runs. Only UNIMPLEMENTED can have come from the service
// itself, so the code is what separates "the connection was turned away" from "the handshake was
// delivered and answered".
//
// The Java client asserts the same pair in SidecarHandshakeTest
// (parallel-consumer-proxy-client-java-harness); this is the same claim made from the other side of
// the language boundary, which is the only way to learn that the Go wire mapping agrees with the
// server about what a session looks like.
func TestTheHandshakeReachesTheSessionServiceAndItsRefusalReachesTheCaller(t *testing.T) {
	sidecar, err := harness.EngineLessSidecar()
	if err != nil {
		t.Fatalf("locating the sidecar: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	client, err := parallelconsumer.Open(ctx, parallelconsumer.Options{
		SidecarPath: sidecar.Path,
		SidecarArgs: sidecar.Args,
		Topics:      []string{"handshake-topic"},
		// The sidecar reads no properties at all on this build. Real credentials never belong in a
		// test, and there is nothing here to give them to.
		KafkaProperties: map[string]string{},
		InstanceTag:     "go-handshake",
	})
	if err == nil {
		_ = client.Close()
		t.Fatal("the sidecar hosts no engine, so Open must fail rather than report a configured session")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("no gRPC status in the failure, so the transport never reached the wire: %v", err)
	}
	if st.Code() != codes.Unimplemented {
		t.Fatalf("handshake failed with %s, want %s - UNIMPLEMENTED is the only code the session "+
			"SERVICE raises, so it is what proves the Configure was delivered rather than turned "+
			"away by an interceptor: %v", st.Code(), codes.Unimplemented, err)
	}
	if !strings.Contains(st.Message(), harness.NoEngineDescription) {
		t.Errorf("the refusal did not name what is missing (%q), so a client author would debug "+
			"their own code; got %q", harness.NoEngineDescription, st.Message())
	}
}

// TestASidecarThatIsNotListeningFailsDifferentlyFromOneThatRefuses is the control arm, and it is
// permanent rather than a one-off demonstration: pointed at a port nothing is listening on, the
// same client fails in a way that is not the refusal above. Without it, the test that matters could
// be passing on any failure at all - which is the shape of an assertion that cannot fail for the
// reason it names.
//
// The sidecar it spawns is `true`, which announces no port; that is a different failure again, so
// this one uses the library's own connect path against a dead port by spawning a shell that
// announces a port nothing serves.
func TestASidecarThatIsNotListeningFailsDifferentlyFromOneThatRefuses(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving a port: %v", err)
	}
	deadPort := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("releasing the reserved port: %v", err)
	}

	announcer, err := writeAnnouncer(t, deadPort)
	if err != nil {
		t.Fatalf("writing the announcer: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client, err := parallelconsumer.Open(ctx, parallelconsumer.Options{
		SidecarPath:     announcer,
		Topics:          []string{"handshake-topic"},
		KafkaProperties: map[string]string{},
		InstanceTag:     "go-handshake-control",
	})
	if err == nil {
		_ = client.Close()
		t.Fatal("nothing is listening on that port, so Open cannot have succeeded")
	}
	if st, ok := status.FromError(err); ok && st.Code() == codes.Unimplemented {
		t.Fatalf("nothing answered, so nothing can have refused, yet the failure was %s: %v",
			st.Code(), err)
	}
}

// writeAnnouncer writes a sidecar that announces a port and then holds its stdin, which is the
// spawning contract's whole client-visible surface. It exists so the control arm above drives the
// library's REAL connect path at a dead port, rather than testing what happens when a child prints
// nothing - a different failure that would prove nothing about the assertion it controls.
//
// printf and read are shell builtins, so this is a single process holding its own lifeline; no
// grandchild survives the library's reap.
func writeAnnouncer(t *testing.T, port int) (string, error) {
	t.Helper()
	script := filepath.Join(t.TempDir(), "announcer.sh")
	body := fmt.Sprintf("#!/bin/sh\nprintf 'port: %d\\n'\nwhile read -r _ignored; do :; done\nexit 0\n", port)
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		return "", err
	}
	return script, nil
}
