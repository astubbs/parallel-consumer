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
	"sync/atomic"
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
	stream  sessionTransport
	cancel  context.CancelFunc
	session Session

	// sendMu serializes stream sends. gRPC allows one Send at a time on a stream, and every
	// executor reports on this one stream.
	sendMu sync.Mutex

	// queue is the client-side dispatch queue. Its capacity is the proxy's own in-flight ceiling,
	// as the STRUCTURAL statement of the bound - but it is not what admits a record, and it can
	// never be the thing that fires. unresolved is.
	queue chan *proxyv1.DispatchRecord

	// unresolved counts the records this client has been dispatched and has not yet reported -
	// QUEUED PLUS EXECUTING. That, and never the queue's own occupancy, is what max_concurrency
	// bounds: handing a record to an executor MOVES it and does not free its slot, so a client
	// counting queued records alone has room exactly when it should be raising a violation.
	unresolved atomic.Int64

	stopHandout chan struct{}
	executors   sync.WaitGroup
	receiver    sync.WaitGroup

	polled    bool
	pollMu    sync.Mutex
	closeOnce sync.Once
	closeErr  error

	// closed is THE SESSION'S END, not Close's. It is closed by endSession, from whichever path
	// reaches the end first, and stopOnce/endOnce are what let more than one path reach it: a
	// stream that faulted is followed by an application calling Close, and closing either channel
	// twice would panic.
	stopOnce sync.Once
	endOnce  sync.Once
	closed   chan struct{}

	// failMu guards failure. Err is read from the application's goroutine and written from the
	// receive loop and from shutdown, so the read needs the same lock the write takes - the channel
	// close alone orders only the goroutine that did the closing.
	failMu  sync.Mutex
	failure error
}

// Open spawns the sidecar, connects to it and completes the fresh-session handshake. It returns
// once the proxy's effective configuration has arrived - only then is the session open.
//
// ctx bounds the spawn and handshake only. The session itself lives until Close.
func Open(ctx context.Context, opts Options) (*Client, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}

	if opts.Embedded {
		return openEmbedded(ctx, opts)
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

// Done is closed when the session has ENDED, by any route: the application closing the client, the
// proxy completing the stream, the sidecar going away, or the stream faulting mid-session. Err
// reports why.
//
// It deliberately does not require a Close to fire. A surface where the session can die while the
// application still believes it is consuming is the worst failure this API could have, so the
// session's end is observable on its own.
func (c *Client) Done() <-chan struct{} { return c.closed }

// Err reports the session's first fatal error, if any - nil when the session ended cleanly. It is
// meaningful once Done is closed: the cause is written before the close, so anything that observed
// Done observes the cause.
func (c *Client) Err() error {
	c.failMu.Lock()
	defer c.failMu.Unlock()
	return c.failure
}

// receive is the admin loop. IT ALWAYS READS. Backpressure is never applied by not reading: the
// stream also carries the control plane, so an admin that stops reading head-of-line-blocks
// itself.
func (c *Client) receive() {
	defer c.receiver.Done()
	for {
		msg, err := c.stream.Recv()
		if err != nil {
			// THE STREAM ENDING IS THE SESSION ENDING, and ending it here is what makes Done fire
			// and Err meaningful without the application having to Close the client to find out.
			// A drain that completed and a stream that faulted are both ends; only the second has
			// a cause.
			if isSessionEnd(err) {
				c.endSession(nil)
			} else {
				c.endSession(fmt.Errorf("parallelconsumer: session stream: %w", err))
			}
			return
		}
		switch m := msg.GetMessage().(type) {
		case *proxyv1.ProxyMessage_Dispatch:
			if err := c.enqueue(m.Dispatch); err != nil {
				c.endSession(err)
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
//
// ADMISSION IS THE UNRESOLVED COUNT, never the channel's free space. The proxy's worked example is
// the case that separates them: at a ceiling of three with A, B and C dispatched and two of them
// already out with executors, the channel has two free slots and the ceiling has none.
func (c *Client) enqueue(d *proxyv1.Dispatch) error {
	for _, rec := range d.GetRecords() {
		// Read then add, rather than add then check: enqueue has exactly one caller, the receive
		// loop, so admission is single-producer, and a concurrent settle can only free capacity -
		// never take it. It also leaves the count honest in the violation's own message.
		already := c.unresolved.Load()
		if already >= int64(c.session.MaxConcurrency) {
			return c.overflow(already, rec)
		}
		c.unresolved.Add(1)
		select {
		case c.queue <- rec:
		default:
			// Unreachable while the count above is right - queued records are a subset of the
			// unresolved ones, so the channel fills only once the ceiling is already reached. Kept
			// as the structural backstop, answering exactly as the counted check does.
			c.settle()
			return c.overflow(already, rec)
		}
	}
	return nil
}

// overflow is the counted protocol violation: the proxy dispatched past the in-flight ceiling it
// declared itself. Never a load condition, so never drop a record and never grow the queue.
//
// The token is rendered AS IT ARRIVED. Opacity forbids deriving from a token - parsing its
// record_id, comparing epochs, branching on either - and says nothing about printing one; a Token
// carries engine-generated identity and no credentials, unlike the Configure message.
func (c *Client) overflow(alreadyUnresolved int64, rec *proxyv1.DispatchRecord) error {
	return fmt.Errorf("parallelconsumer: the proxy dispatched a record while %d were already "+
		"unresolved - queued plus executing - past the max_concurrency of %d it declared itself, so "+
		"this is a protocol violation and not load; the call is cancelled rather than failed with %s, "+
		"because a gRPC client cannot set a status. The overflowing record's token, as it arrived: %v",
		alreadyUnresolved, c.session.MaxConcurrency, codes.FailedPrecondition, rec.GetToken())
}

// settle records that one record reached a verdict and no longer counts against the ceiling.
//
// ONLY A VERDICT FREES A SLOT. There are two this client can reach today: a report was sent, and a
// queued record was discarded at shutdown. Taking a record off the queue is not one of them. The
// rest - a Released at shutdown, a Drop for a record this client still holds, and WorkerDied - are
// gated by capabilities this client does not declare, and whoever implements them owes a settle at
// each.
//
// Saturating, so that a double settle can never wrap the count into a ceiling nothing could
// overflow, which is the defect this counter exists to remove.
func (c *Client) settle() {
	for {
		unresolved := c.unresolved.Load()
		if unresolved == 0 {
			return
		}
		if c.unresolved.CompareAndSwap(unresolved, unresolved-1) {
			return
		}
	}
}

// discardQueue drops every queued record and settles it.
//
// This session negotiated only dispatch, so a queued record may be neither run nor Released at
// shutdown - it is discarded, its offset never commits, and the proxy returns it to scheduling on
// its own. Nothing will ever report it, so this is the only thing that can free its slot.
func (c *Client) discardQueue() {
	for {
		select {
		case <-c.queue:
			c.settle()
		default:
			return
		}
	}
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
	// The record stops counting against the in-flight ceiling once it has been REPORTED, which is
	// where this deferred call runs - never when the executor picked it up. It is a defer rather
	// than a line after the send so that an executor dying mid-record cannot skip it; a skipped
	// decrement shrinks the ceiling permanently, one slot per crash, until this client declares a
	// protocol violation against a proxy doing nothing wrong.
	defer c.settle()

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
// Calling Close more than once is safe; later calls wait for the first and return its result. So
// is calling it on a session that has already ended by itself - which is the ordinary way to reap
// the sidecar after Done fires.
func (c *Client) Close() error {
	// sync.Once.Do returns only once the function has completed, for every caller, so a second
	// Close waits for the first and sees its result without needing a channel of its own. It must
	// not wait on `closed`: that now fires at the session's end, which can be long before - or
	// entirely without - a Close.
	c.closeOnce.Do(func() {
		c.closeErr = c.shutdown()
	})
	return c.closeErr
}

func (c *Client) shutdown() error {
	c.stopHandingOut() // stop hand-out; executing records keep running

	c.executors.Wait() // executing records finish and report normally

	// Whatever is still queued is dropped - never run, never given a verdict this client did not
	// reach. Discarding it here rather than leaving it to the garbage collector is what keeps the
	// unresolved count honest, since these records will never be reported.
	c.discardQueue()

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

	if c.conn != nil {
		_ = c.conn.Close()
	}

	// Closing the lifecycle pipe is the reap. Never kill a sidecar with the stream still open -
	// that turns a clean drain into a reconnect-window recovery for the next group member.
	if c.side != nil {
		if err := c.side.stop(defaultReapGrace); err != nil {
			c.fail(err)
		}
	}

	// The session has ended by the application's own hand. Done fires here rather than in Close, so
	// that every route to the end goes through one place.
	c.endSession(nil)
	return c.Err()
}

// endSession ends the session exactly once, from whichever path gets there first: the stream
// faulting, the stream completing, an overflow this client refused, or Close finishing its
// shutdown. It is the ONLY place `closed` is closed.
//
// Both halves of the client-authoring guide's §1 rule are delivered here - THAT the session ended,
// and WHY - and the order matters: the cause is recorded before the close, so a caller that saw
// Done sees the cause too.
func (c *Client) endSession(cause error) {
	if cause != nil {
		c.fail(cause)
	}
	// Hand-out stops first. Executors park in a select on stopHandout, and closing it is what
	// releases them; without it they wait for a record no live receive loop will ever queue.
	c.stopHandingOut()
	c.endOnce.Do(func() { close(c.closed) })
}

// stopHandingOut releases every executor waiting for a record. Idempotent, because both Close and
// the session's own end reach it, in either order.
func (c *Client) stopHandingOut() {
	c.stopOnce.Do(func() { close(c.stopHandout) })
}

// fail records the session's first fatal error. The first one wins: later errors are usually its
// consequences, and the cause explains them.
func (c *Client) fail(err error) {
	c.failMu.Lock()
	defer c.failMu.Unlock()
	if c.failure == nil {
		c.failure = err
	}
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
