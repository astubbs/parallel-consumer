// Copyright (C) 2026 Antony Stubbs and contributors

//go:build pcffi

package parallelconsumer

/*
#cgo CFLAGS: -I${SRCDIR}/../ffi/build
#cgo LDFLAGS: -L${SRCDIR}/../ffi/build -lpc -Wl,-rpath,${SRCDIR}/../ffi/build
#include <libpc.h>
#include <graal_isolate.h>
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"unsafe"

	proxyv1 "github.com/astubbs/parallel-consumer/parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/gen/parallelconsumer/proxy/v1"
	"google.golang.org/protobuf/proto"
)

// The codes PcSession.java returns. A bare -4 in an error message is unreadable, and these are the
// only place the two sides' numbering is written down together.
const (
	ffiOK              = 0
	ffiErrNoSession    = -1
	ffiErrBufferTooSmall = -2
	ffiErrTimeout      = -3
	ffiErrSessionEnded = -4
	ffiErrBadFrame     = -5
	ffiErrInternal     = -6
)

func ffiCodeName(code int) string {
	switch code {
	case ffiOK:
		return "OK"
	case ffiErrNoSession:
		return "ERR_NO_SESSION"
	case ffiErrBufferTooSmall:
		return "ERR_BUFFER_TOO_SMALL"
	case ffiErrTimeout:
		return "ERR_TIMEOUT"
	case ffiErrSessionEnded:
		return "ERR_SESSION_ENDED"
	case ffiErrBadFrame:
		return "ERR_BAD_FRAME"
	case ffiErrInternal:
		return "ERR_INTERNAL"
	default:
		return fmt.Sprintf("unknown(%d)", code)
	}
}

// recvPollMillis bounds one pc_next call. Recv must look blocking to its caller, so a timeout is
// not an end-of-stream - it is a reason to ask again. Short enough that CloseSend is noticed
// promptly, long enough not to spin.
const recvPollMillis = 200

// embeddedTransport carries a session over the C ABI instead of over gRPC. It implements
// sessionTransport, which is the only thing the client knows about either.
type embeddedTransport struct {
	isolate *C.graal_isolate_t
	handle  C.longlong

	closeOnce sync.Once
	closed    chan struct{}
}

// thread returns an isolate thread valid for the CURRENT OS thread.
//
// This lookup cannot be hoisted into a field. A GraalVM isolate thread belongs to the OS thread it
// was attached on, and Go migrates goroutines between OS threads at will, so a cached pointer would
// be silently wrong the first time the scheduler moved the caller - and the failure mode is memory
// corruption, not an error. Looking it up per call is the price of living in a runtime that owns
// its own threads.
func (t *embeddedTransport) thread() (*C.graal_isolatethread_t, error) {
	if thread := C.graal_get_current_thread(t.isolate); thread != nil {
		return thread, nil
	}
	var thread *C.graal_isolatethread_t
	if rc := C.graal_attach_thread(t.isolate, &thread); rc != 0 {
		return nil, fmt.Errorf("parallelconsumer: attaching this thread to the isolate failed with %d", int(rc))
	}
	return thread, nil
}

func (t *embeddedTransport) Send(msg *proxyv1.ClientMessage) error {
	frame, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("parallelconsumer: marshalling a client message: %w", err)
	}
	thread, err := t.thread()
	if err != nil {
		return err
	}
	var first *C.char
	if len(frame) > 0 {
		first = (*C.char)(unsafe.Pointer(&frame[0]))
	}
	rc := int(C.pc_send(thread, t.handle, first, C.int(len(frame))))
	switch rc {
	case ffiOK:
		return nil
	case ffiErrSessionEnded, ffiErrNoSession:
		return io.EOF
	default:
		return fmt.Errorf("parallelconsumer: pc_send returned %s: %s", ffiCodeName(rc), t.lastError(thread))
	}
}

func (t *embeddedTransport) Recv() (*proxyv1.ProxyMessage, error) {
	buf := make([]byte, 64*1024)
	for {
		select {
		case <-t.closed:
			return nil, io.EOF
		default:
		}
		thread, err := t.thread()
		if err != nil {
			return nil, err
		}
		var written C.int
		rc := int(C.pc_next(thread, t.handle, (*C.char)(unsafe.Pointer(&buf[0])), C.int(len(buf)),
			&written, C.int(recvPollMillis)))
		switch rc {
		case ffiOK:
			msg := &proxyv1.ProxyMessage{}
			if err := proto.Unmarshal(buf[:int(written)], msg); err != nil {
				return nil, fmt.Errorf("parallelconsumer: unmarshalling a proxy message: %w", err)
			}
			return msg, nil
		case ffiErrTimeout:
			// Idle, not ended. Ask again.
			continue
		case ffiErrBufferTooSmall:
			// The frame is still queued; written carries the size it needs. Growing and retrying is
			// why pc_next puts the frame back rather than dropping it.
			buf = make([]byte, int(written))
			continue
		case ffiErrSessionEnded, ffiErrNoSession:
			return nil, io.EOF
		default:
			return nil, fmt.Errorf("parallelconsumer: pc_next returned %s: %s", ffiCodeName(rc), t.lastError(thread))
		}
	}
}

func (t *embeddedTransport) CloseSend() error {
	var err error
	t.closeOnce.Do(func() {
		close(t.closed)
		thread, attachErr := t.thread()
		if attachErr != nil {
			err = attachErr
			return
		}
		if rc := int(C.pc_session_close(thread, t.handle)); rc != ffiOK && rc != ffiErrNoSession {
			err = fmt.Errorf("parallelconsumer: pc_session_close returned %s", ffiCodeName(rc))
		}
	})
	return err
}

func (t *embeddedTransport) lastError(thread *C.graal_isolatethread_t) string {
	buf := make([]byte, 8192)
	n := int(C.pc_last_error(thread, t.handle, (*C.char)(unsafe.Pointer(&buf[0])), C.int(len(buf))))
	if n <= 0 {
		return "(no detail recorded)"
	}
	return string(buf[:n])
}

// theIsolate is created once per process. Isolates are heap-sized VM instances; one per session
// would multiply the footprint for no benefit, and sessions are already separated by handle.
var (
	isolateOnce sync.Once
	theIsolate  *C.graal_isolate_t
	isolateErr  error
)

func ensureIsolate() (*C.graal_isolate_t, error) {
	isolateOnce.Do(func() {
		var isolate *C.graal_isolate_t
		var thread *C.graal_isolatethread_t
		if rc := C.graal_create_isolate(nil, &isolate, &thread); rc != 0 {
			isolateErr = fmt.Errorf("parallelconsumer: graal_create_isolate failed with %d", int(rc))
			return
		}
		theIsolate = isolate
	})
	return theIsolate, isolateErr
}

func openEmbedded(ctx context.Context, opts Options) (*Client, error) {
	isolate, err := ensureIsolate()
	if err != nil {
		return nil, err
	}
	transport := &embeddedTransport{isolate: isolate, closed: make(chan struct{})}

	thread, err := transport.thread()
	if err != nil {
		return nil, err
	}
	handle := C.pc_session_open(thread)
	if handle <= 0 {
		return nil, fmt.Errorf("parallelconsumer: pc_session_open returned %s", ffiCodeName(int(handle)))
	}
	transport.handle = handle

	// conn and side stay nil: there is no connection and no child process. The teardown path asks
	// whether they exist rather than assuming, which is the whole change the embedded transport
	// needed from the client.
	c := &Client{
		opts:        opts,
		stream:      transport,
		cancel:      func() {},
		stopHandout: make(chan struct{}),
		closed:      make(chan struct{}),
	}
	if err := c.handshake(ctx); err != nil {
		_ = transport.CloseSend()
		return nil, err
	}
	return c, nil
}

var _ sessionTransport = (*embeddedTransport)(nil)
var _ = errors.New
