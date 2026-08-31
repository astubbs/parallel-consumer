// Copyright (C) 2026 Antony Stubbs and contributors

package parallelconsumer

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	proxyv1 "github.com/astubbs/parallel-consumer/parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/gen/parallelconsumer/proxy/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// settle is how long a test waits for something that should happen promptly. Generous, because the
// alternative on a loaded machine is a flake, and every wait here is for an event the code is
// supposed to produce immediately rather than a race against one.
const settle = 5 * time.Second

// fakeStream is the session stream, scripted. It exists because the two properties these tests are
// about - what happens when the stream ENDS, and what the queue admits - are decided entirely by
// what Recv returns, and no sidecar can be made to return a chosen error on demand.
//
// It satisfies proxyv1.ProxyService_SessionClient, which is grpc.BidiStreamingClient: Send, Recv
// and the ClientStream methods, the last of which this client never calls.
type fakeStream struct {
	mu   sync.Mutex
	sent []*proxyv1.ClientMessage

	// recvs is what Recv hands out, in order. A test writes the messages and then the terminal
	// error; Recv blocks on an empty channel exactly as a live stream blocks on a quiet proxy.
	recvs chan recvResult
}

type recvResult struct {
	msg *proxyv1.ProxyMessage
	err error
}

func newFakeStream() *fakeStream {
	return &fakeStream{recvs: make(chan recvResult, 16)}
}

func (f *fakeStream) Send(msg *proxyv1.ClientMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, msg)
	return nil
}

func (f *fakeStream) Recv() (*proxyv1.ProxyMessage, error) {
	r := <-f.recvs
	return r.msg, r.err
}

func (f *fakeStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (f *fakeStream) Trailer() metadata.MD         { return metadata.MD{} }
func (f *fakeStream) CloseSend() error             { return nil }
func (f *fakeStream) Context() context.Context     { return context.Background() }
func (f *fakeStream) SendMsg(any) error            { return nil }
func (f *fakeStream) RecvMsg(any) error            { return nil }

// newTestClient builds a client around a scripted stream, at the state Open leaves it in: the
// handshake done, the effective session known, the queue sized. It has no sidecar and no
// connection, so it must not be Closed - these tests exercise the session's end, which is exactly
// the thing that must not require a Close to observe.
func newTestClient(stream proxyv1.ProxyService_SessionClient, maxConcurrency, executorCount int32) *Client {
	return &Client{
		stream: stream,
		cancel: func() {},
		session: Session{
			MaxConcurrency: maxConcurrency,
			ExecutorCount:  executorCount,
			Capabilities:   []string{CapabilityDispatch},
		},
		queue:       make(chan *proxyv1.DispatchRecord, maxConcurrency),
		stopHandout: make(chan struct{}),
		closed:      make(chan struct{}),
	}
}

// startExecutors starts executors the way Poll does, with a processor that always succeeds.
func startExecutors(c *Client, ctx context.Context) {
	for i := int32(0); i < c.session.ExecutorCount; i++ {
		c.executors.Add(1)
		go c.execute(ctx, func(context.Context, InboundRecord) (Outcome, error) { return Succeed(), nil })
	}
}

func waitGroupWithin(t *testing.T, wg *sync.WaitGroup, what string) {
	t.Helper()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(settle):
		t.Fatalf("%s never finished - the session ended and they were left parked", what)
	}
}

// TestAStreamFaultEndsTheSessionAndSaysWhy is the P0 the cross-client divergence review found: the
// session can die and nobody can tell. Every other client fires its end-of-session surface on a
// mid-session stream error; Go closed `closed` in exactly one place, inside Close, so on a stream
// fault Done never fired, Err never became meaningful, and the executors parked forever in a select
// where neither case could be ready.
func TestAStreamFaultEndsTheSessionAndSaysWhy(t *testing.T) {
	stream := newFakeStream()
	c := newTestClient(stream, 4, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startExecutors(c, ctx)
	c.receiver.Add(1)
	go c.receive()

	stream.recvs <- recvResult{err: status.Error(codes.Internal, "the proxy fell over")}

	select {
	case <-c.Done():
	case <-time.After(settle):
		t.Fatal("the stream faulted and Done never fired - an application cannot learn the session died")
	}

	err := c.Err()
	if err == nil {
		t.Fatal("Done fired with no cause - the caller learns THAT the session ended but not WHY")
	}
	if !strings.Contains(err.Error(), "the proxy fell over") {
		t.Errorf("the cause did not carry the stream's own error: %v", err)
	}

	waitGroupWithin(t, &c.executors, "the executors")
	waitGroupWithin(t, &c.receiver, "the receive loop")
}

// TestACompletedStreamEndsTheSessionWithoutAFault is the other half: a drain that completed is the
// end of the session just as much as a fault is, and it must not be reported as a failure.
func TestACompletedStreamEndsTheSessionWithoutAFault(t *testing.T) {
	stream := newFakeStream()
	c := newTestClient(stream, 4, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startExecutors(c, ctx)
	c.receiver.Add(1)
	go c.receive()

	stream.recvs <- recvResult{err: io.EOF}

	select {
	case <-c.Done():
	case <-time.After(settle):
		t.Fatal("the proxy completed the stream and Done never fired")
	}
	if err := c.Err(); err != nil {
		t.Errorf("an ordinary end of session was reported as a failure: %v", err)
	}
	waitGroupWithin(t, &c.executors, "the executors")
}

// TestEndingTheSessionTwiceIsSafe covers the path a real application takes after a stream fault:
// it sees Done, then calls Close, whose shutdown ends the session again. Two closes of one channel
// panic, so the once-guards are the whole of what keeps that from being a crash - and the first
// cause is the one that survives, because it is the one that explains the rest.
func TestEndingTheSessionTwiceIsSafe(t *testing.T) {
	c := newTestClient(newFakeStream(), 1, 1)

	c.endSession(status.Error(codes.Internal, "the first cause"))
	c.endSession(status.Error(codes.Internal, "a later, derived one"))
	c.stopHandingOut()

	if err := c.Err(); err == nil || !strings.Contains(err.Error(), "the first cause") {
		t.Errorf("the first fatal cause did not survive: %v", err)
	}
	select {
	case <-c.Done():
	default:
		t.Error("Done did not fire")
	}
}
