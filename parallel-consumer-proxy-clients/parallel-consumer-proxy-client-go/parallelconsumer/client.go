// Copyright (C) 2026 Antony Stubbs and contributors

// Package parallelconsumer is a Go client for the Parallel Consumer language proxy.
//
// The shape, which is the same in every language:
//
//	application process
//	├── the user's function (an ordinary Go func - the proxy never learns what it is)
//	├── this library
//	│   ├── admin      - spawns the sidecar, holds the ONE gRPC stream, owns the dispatch queue
//	│   └── executors  - goroutines, each: take record -> run the function -> report the outcome
//	└── sidecar proxy (child process) - runs Parallel Consumer, owns Kafka entirely
//
// Goroutines are the whole of Go's part in that: no worker processes, no fork hazard, and nothing
// to keep gRPC away from - which is the point of specifying the shape rather than the mechanism.
//
// The library is STATELESS PER RECORD. The fencing token rides from dispatch to report on the
// executing goroutine's stack and is echoed byte-identically; there is no request map, no dedupe
// cache and no completion registry, because a client that holds no per-record state cannot have a
// per-record state bug. Fencing is the proxy's job.
package parallelconsumer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	proxyv1 "github.com/astubbs/parallel-consumer/parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/gen/parallelconsumer/proxy/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// defaultReapGrace bounds how long Close waits for the proxy to complete the stream and for the
// sidecar to exit, before it stops being polite about it.
const defaultReapGrace = 15 * time.Second

// ErrAlreadyPolling is returned by a second Poll on one client. The poll-with-a-function shape is
// at most once per client.
var ErrAlreadyPolling = errors.New("parallelconsumer: Poll has already been called on this client")

// Client is one session: one sidecar process, one gRPC stream, one dispatch queue.
//
// Open connects and completes the handshake; Poll starts processing and returns; Close performs
// the client-initiated shutdown. Poll does not block, so an application that wants to wait until
// the session ends waits on Done.
type Client struct {
	opts    Options
	side    *sidecar
	conn    *grpc.ClientConn
	stream  proxyv1.ProxyService_SessionClient
	cancel  context.CancelFunc
	session Session

	// sendMu serializes stream sends. gRPC allows one Send at a time on a stream, and every
	// executor reports on this one stream.
	sendMu sync.Mutex

	// queue is the client-side dispatch queue. Its depth is the proxy's own in-flight ceiling, so
	// in a correct system it cannot overflow; an overflow is a protocol violation, not load.
	queue chan *proxyv1.DispatchRecord

	stopHandout chan struct{}
	executors   sync.WaitGroup
	receiver    sync.WaitGroup

	polled    bool
	pollMu    sync.Mutex
	closeOnce sync.Once
	closed    chan struct{}
	closeErr  error

	failOnce sync.Once
	failure  error
}

// Open spawns the sidecar, connects to it and completes the fresh-session handshake. It returns
// once the proxy's effective configuration has arrived - only then is the session open.
//
// ctx bounds the spawn and handshake only. The session itself lives until Close.
func Open(ctx context.Context, opts Options) (*Client, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}

	side, err := startSidecar(ctx, opts)
	if err != nil {
		return nil, err
	}

	// The target is the ordinary host:port authority the proxy's loopback allowlist expects; no
	// TLS, no interceptors, no load balancing - the protocol uses a deliberately narrow slice of
	// gRPC so that every language's implementation suffices.
	target := fmt.Sprintf("127.0.0.1:%d", side.port)
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		_ = side.stop(defaultReapGrace)
		return nil, fmt.Errorf("parallelconsumer: connecting to the sidecar: %w", err)
	}

	// The stream outlives ctx, so it gets its own cancellable context; cancelling it is what
	// terminates the session if the polite shutdown does not complete.
	streamCtx, cancel := context.WithCancel(context.Background())
	stream, err := proxyv1.NewProxyServiceClient(conn).Session(streamCtx)
	if err != nil {
		cancel()
		_ = conn.Close()
		_ = side.stop(defaultReapGrace)
		return nil, fmt.Errorf("parallelconsumer: opening the session stream: %w", err)
	}

	c := &Client{
		opts:        opts,
		side:        side,
		conn:        conn,
		stream:      stream,
		cancel:      cancel,
		stopHandout: make(chan struct{}),
		closed:      make(chan struct{}),
	}

	if err := c.handshake(ctx); err != nil {
		cancel()
		_ = conn.Close()
		_ = side.stop(defaultReapGrace)
		return nil, err
	}
	return c, nil
}

func (c *Client) handshake(ctx context.Context) error {
	// NOTE the error text: never the configuration itself. Configure carries kafka_properties, and
	// a natural rendering of the message would put credentials in a log line.
	if err := c.send(&proxyv1.ClientMessage{Message: &proxyv1.ClientMessage_Configure{Configure: c.opts.configure()}}); err != nil {
		return fmt.Errorf("parallelconsumer: sending Configure: %w", err)
	}

	msg, err := c.recvWithin(ctx)
	if err != nil {
		return fmt.Errorf("parallelconsumer: awaiting Configured: %w", err)
	}
	configured, ok := msg.GetMessage().(*proxyv1.ProxyMessage_Configured)
	if !ok {
		return fmt.Errorf("parallelconsumer: handshake reply was %T, not Configured", msg.GetMessage())
	}

	effective := configured.Configured
	// Absence here is a protocol violation, never "unlimited": the ceiling is always finite and
	// always reported, and it is also this client's queue depth, so there is nothing to fall back
	// on.
	if effective.MaxConcurrency == nil || effective.GetMaxConcurrency() < 1 {
		return errors.New("parallelconsumer: Configured carried no usable max_concurrency - the in-flight ceiling is always reported")
	}
	if effective.ExecutorCount == nil || effective.GetExecutorCount() < 1 {
		return errors.New("parallelconsumer: Configured carried no usable executor_count")
	}

	c.session = sessionOf(effective)
	c.queue = make(chan *proxyv1.DispatchRecord, effective.GetMaxConcurrency())
	return nil
}

// recvWithin waits for one proxy message, honouring ctx. stream.Recv is not itself
// context-aware once the stream exists, so the wait is done off-goroutine.
func (c *Client) recvWithin(ctx context.Context) (*proxyv1.ProxyMessage, error) {
	type result struct {
		msg *proxyv1.ProxyMessage
		err error
	}
	done := make(chan result, 1)
	go func() {
		msg, err := c.stream.Recv()
		done <- result{msg, err}
	}()
	select {
	case r := <-done:
		return r.msg, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Session is the effective configuration this session is running with - what the proxy replied,
// including the negotiated capability set. Assert on this, never on the Options.
func (c *Client) Session() Session { return c.session }

// Poll starts processing with the user's function and returns immediately. At most once per
// client.
//
// It returns rather than blocking because the session has no natural end: an application that
// wants to wait for one waits on Done. ctx is handed to every invocation of the function, and
// cancelling it shuts the session down the same way Close does.
func (c *Client) Poll(ctx context.Context, processor Processor) error {
	if processor == nil {
		return errors.New("parallelconsumer: Poll needs a processor")
	}
	c.pollMu.Lock()
	if c.polled {
		c.pollMu.Unlock()
		return ErrAlreadyPolling
	}
	c.polled = true
	c.pollMu.Unlock()

	for i := int32(0); i < c.session.ExecutorCount; i++ {
		c.executors.Add(1)
		go c.execute(ctx, processor)
	}

	c.receiver.Add(1)
	go c.receive()

	go func() {
		select {
		case <-ctx.Done():
			_ = c.Close()
		case <-c.closed:
		}
	}()
	return nil
}

// Done is closed when the session has finished shutting down. Err reports why.
func (c *Client) Done() <-chan struct{} { return c.closed }

// Err reports the session's first fatal error, if any. It is meaningful once Done is closed.
func (c *Client) Err() error { return c.failure }

// receive is the admin loop. IT ALWAYS READS. Backpressure is never applied by not reading: the
// stream also carries the control plane, so an admin that stops reading head-of-line-blocks
// itself.
func (c *Client) receive() {
	defer c.receiver.Done()
	for {
		msg, err := c.stream.Recv()
		if err != nil {
			if !isSessionEnd(err) {
				c.fail(fmt.Errorf("parallelconsumer: session stream: %w", err))
			}
			return
		}
		switch m := msg.GetMessage().(type) {
		case *proxyv1.ProxyMessage_Dispatch:
			if err := c.enqueue(m.Dispatch); err != nil {
				c.fail(err)
				c.cancel() // the client fails the stream; there is no way back from an overflow
				return
			}
		default:
			// Every remaining proxy message is gated by a capability this client does not declare
			// yet, and the rule for an un-negotiated message is that the receiver never acts on
			// it. Ignoring is the non-fatal half of that rule.
			c.noteUnnegotiated(msg)
		}
	}
}

// enqueue queues a wave in record order. Hand-out is FIFO, by arrival and then by the wave's own
// order, which a buffered channel gives directly.
func (c *Client) enqueue(d *proxyv1.Dispatch) error {
	for _, rec := range d.GetRecords() {
		select {
		case c.queue <- rec:
		default:
			// The queue's depth IS the proxy's declared ceiling, so a full queue means the proxy
			// exceeded it. That is a protocol violation, never a load condition: never drop a
			// record, never grow the queue.
			return fmt.Errorf("parallelconsumer: dispatch overflowed the client queue at its "+
				"max_concurrency of %d - protocol violation (%s)", c.session.MaxConcurrency, codes.FailedPrecondition)
		}
	}
	return nil
}

// execute is one executor goroutine: take a record, run the function, report the outcome.
func (c *Client) execute(ctx context.Context, processor Processor) {
	defer c.executors.Done()
	for {
		select {
		case <-c.stopHandout:
			return
		case rec := <-c.queue:
			c.runOne(ctx, processor, rec)
		}
	}
}

func (c *Client) runOne(ctx context.Context, processor Processor, rec *proxyv1.DispatchRecord) {
	outcome, err := invoke(ctx, processor, inboundOf(rec))

	// The token is echoed VERBATIM - the same message the proxy sent, never one this client
	// rebuilt from parsed parts. It is opaque: nothing here reads record_id or compares epochs.
	report := &proxyv1.Report{Token: rec.GetToken()}
	if err != nil {
		reason := err.Error()
		// Report_Failure_ with the trailing underscore is the ONEOF WRAPPER; Report_Failure without
		// it is the nested message. protoc-gen-go disambiguates the collision that way, and the two
		// spellings are one keystroke apart.
		report.Outcome = &proxyv1.Report_Failure_{Failure: &proxyv1.Report_Failure{Reason: &reason}}
	} else {
		success := &proxyv1.Report_Success{}
		for _, out := range outcome.produce {
			success.Produce = append(success.Produce, out.produceRecord())
		}
		report.Outcome = &proxyv1.Report_Success_{Success: success}
	}

	if err := c.send(&proxyv1.ClientMessage{Message: &proxyv1.ClientMessage_Report{Report: report}}); err != nil {
		if !isSessionEnd(err) {
			c.fail(fmt.Errorf("parallelconsumer: reporting an outcome: %w", err))
		}
	}
}

// invoke runs the user's function and turns a panic into a failure. A crashing worker must produce
// a failure report, not tear down the stream.
func invoke(ctx context.Context, processor Processor, record InboundRecord) (outcome Outcome, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("the processor panicked: %v", r)
		}
	}()
	return processor(ctx, record)
}

func (c *Client) send(msg *proxyv1.ClientMessage) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	return c.stream.Send(msg)
}

// Close performs the client-initiated shutdown: stop handing records out, let executing records
// finish and report, then half-close the stream. The half-close IS the shutdown signal - there is
// no shutdown-request message. Then the sidecar is reaped by closing its lifecycle pipe.
//
// Calling Close more than once is safe; later calls wait for the first and return its result.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.shutdown()
		close(c.closed)
	})
	<-c.closed
	return c.closeErr
}

func (c *Client) shutdown() error {
	close(c.stopHandout) // stop hand-out; executing records keep running

	c.executors.Wait() // executing records finish and report normally

	// Half-close: no more sends. Nothing left to say - everything run has been reported.
	if err := c.stream.CloseSend(); err != nil && !isSessionEnd(err) {
		c.fail(fmt.Errorf("parallelconsumer: half-closing the stream: %w", err))
	}

	// Give the proxy its drain: it commits, completes the stream, and the receive loop ends.
	waited := make(chan struct{})
	go func() { c.receiver.Wait(); close(waited) }()
	select {
	case <-waited:
	case <-time.After(defaultReapGrace):
	}
	c.cancel()
	c.receiver.Wait()

	_ = c.conn.Close()

	// Closing the lifecycle pipe is the reap. Never kill a sidecar with the stream still open -
	// that turns a clean drain into a reconnect-window recovery for the next group member.
	if err := c.side.stop(defaultReapGrace); err != nil {
		c.fail(err)
	}
	return c.failure
}

func (c *Client) fail(err error) {
	c.failOnce.Do(func() { c.failure = err })
}

// noteUnnegotiated records a message whose capability this session did not negotiate. It is never
// acted on; recording it keeps the violation visible without failing an otherwise healthy stream.
func (c *Client) noteUnnegotiated(msg *proxyv1.ProxyMessage) {
	c.fail(fmt.Errorf("parallelconsumer: proxy sent %T outside the negotiated capability set %v - ignored",
		msg.GetMessage(), c.session.Capabilities))
}

// isSessionEnd reports whether an error is an ordinary end of the session rather than a fault:
// the stream completing, or this client cancelling it during shutdown.
func isSessionEnd(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
		return true
	}
	switch status.Code(err) {
	case codes.Canceled, codes.Unavailable:
		return true
	}
	return false
}
