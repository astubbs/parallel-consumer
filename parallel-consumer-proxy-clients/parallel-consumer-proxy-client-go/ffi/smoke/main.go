// Copyright (C) 2026 Antony Stubbs and contributors

// Smoke test for the FFI session surface: does a Go process, with no sidecar and no gRPC, get a
// Configured frame back out of an embedded Parallel Consumer?
//
// It is deliberately not a demo. It runs the handshake and stops, because the handshake is where
// the two things most likely to break show up: the reflection gaps the --shared build did not
// inherit, and the Kafka client's construction of serialisers from configuration strings.
package main

/*
#cgo CFLAGS: -I${SRCDIR}/../build
#cgo LDFLAGS: -L${SRCDIR}/../build -lpc -Wl,-rpath,${SRCDIR}/../build
#include <libpc.h>
#include <graal_isolate.h>
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"os"
	"unsafe"

	proxyv1 "github.com/astubbs/parallel-consumer/parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/gen/parallelconsumer/proxy/v1"
	"google.golang.org/protobuf/proto"
)

// The error codes PcSession.java returns. Named here because a bare -5 in a failure message is
// unreadable to whoever runs this next.
var codeNames = map[int]string{
	0: "OK", -1: "ERR_NO_SESSION", -2: "ERR_BUFFER_TOO_SMALL", -3: "ERR_TIMEOUT",
	-4: "ERR_SESSION_ENDED", -5: "ERR_BAD_FRAME", -6: "ERR_INTERNAL",
}

func name(code int) string {
	if n, ok := codeNames[code]; ok {
		return n
	}
	return fmt.Sprintf("unknown(%d)", code)
}

func main() {
	broker := os.Getenv("PC_BROKER")
	if broker == "" {
		broker = "localhost:19092"
	}
	topic := os.Getenv("PC_TOPIC")
	if topic == "" {
		topic = "pc-ffi-smoke"
	}

	var isolate *C.graal_isolate_t
	var thread *C.graal_isolatethread_t
	if rc := C.graal_create_isolate(nil, &isolate, &thread); rc != 0 {
		fail("graal_create_isolate rc=%d", int(rc))
	}
	fmt.Println("ok   isolate created")

	handle := C.pc_session_open(thread)
	if handle <= 0 {
		fail("pc_session_open returned %s", name(int(handle)))
	}
	fmt.Printf("ok   pc_session_open -> handle %d\n", int64(handle))

	// A malformed frame must be rejected as such, not swallowed. If this returns OK the parse step
	// is not doing anything and every result below is meaningless.
	if rc := send(thread, handle, []byte{0xff, 0xff, 0xff, 0xff}); rc != -5 {
		fail("a garbage frame returned %s, want ERR_BAD_FRAME", name(rc))
	}
	fmt.Println("ok   a malformed frame is rejected as ERR_BAD_FRAME")

	concurrency := int32(4)
	ordering := proxyv1.ProcessingOrder_PROCESSING_ORDER_UNORDERED
	configure := &proxyv1.Configure{
		Topics:         []string{topic},
		MaxConcurrency: &concurrency,
		Ordering:       &ordering,
		KafkaProperties: map[string]string{
			"bootstrap.servers": broker,
			"group.id":          "pc-ffi-smoke",
			"auto.offset.reset": "earliest",
		},
	}
	frame, err := proto.Marshal(&proxyv1.ClientMessage{
		Message: &proxyv1.ClientMessage_Configure{Configure: configure},
	})
	if err != nil {
		fail("marshalling Configure: %v", err)
	}
	if rc := send(thread, handle, frame); rc != 0 {
		fail("pc_send(Configure) returned %s: %s", name(rc), lastError(thread, handle))
	}
	fmt.Printf("ok   pc_send(Configure) accepted, broker=%s topic=%s\n", broker, topic)

	// Configure builds Kafka clients and starts the engine, so this is the slow one.
	msg, rc := next(thread, handle, 30000)
	if rc != 0 {
		fail("pc_next returned %s: %s", name(rc), lastError(thread, handle))
	}
	var reply proxyv1.ProxyMessage
	if err := proto.Unmarshal(msg, &reply); err != nil {
		fail("unmarshalling the reply: %v", err)
	}
	configured, ok := reply.GetMessage().(*proxyv1.ProxyMessage_Configured)
	if !ok {
		fail("the handshake reply was %T, not Configured", reply.GetMessage())
	}
	fmt.Printf("ok   Configured: max_concurrency=%d executor_count=%d\n",
		configured.Configured.GetMaxConcurrency(), configured.Configured.GetExecutorCount())

	if rc := int(C.pc_session_close(thread, handle)); rc != 0 {
		fail("pc_session_close returned %s", name(rc))
	}
	fmt.Println("ok   session closed")
	fmt.Println("\nPARALLEL CONSUMER RAN INSIDE THIS GO PROCESS - no sidecar, no gRPC, no JVM")
}

func send(thread *C.graal_isolatethread_t, handle C.longlong, frame []byte) int {
	if len(frame) == 0 {
		return 0
	}
	return int(C.pc_send(thread, handle, (*C.char)(unsafe.Pointer(&frame[0])), C.int(len(frame))))
}

func next(thread *C.graal_isolatethread_t, handle C.longlong, timeoutMillis int) ([]byte, int) {
	buf := make([]byte, 64*1024)
	var written C.int
	rc := int(C.pc_next(thread, handle, (*C.char)(unsafe.Pointer(&buf[0])), C.int(len(buf)),
		&written, C.int(timeoutMillis)))
	if rc != 0 {
		return nil, rc
	}
	return buf[:int(written)], 0
}

func lastError(thread *C.graal_isolatethread_t, handle C.longlong) string {
	buf := make([]byte, 8192)
	n := int(C.pc_last_error(thread, handle, (*C.char)(unsafe.Pointer(&buf[0])), C.int(len(buf))))
	if n <= 0 {
		return "(no error recorded)"
	}
	return string(buf[:n])
}

func fail(format string, args ...any) {
	fmt.Printf("FAIL "+format+"\n", args...)
	os.Exit(1)
}
