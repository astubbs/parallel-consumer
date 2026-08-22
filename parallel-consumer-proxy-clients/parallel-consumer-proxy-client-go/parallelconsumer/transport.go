// Copyright (C) 2026 Antony Stubbs and contributors

package parallelconsumer

import (
	proxyv1 "github.com/astubbs/parallel-consumer/parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/gen/parallelconsumer/proxy/v1"
)

// sessionTransport is the whole of what the client needs from whatever carries its session: push a
// frame, pull a frame, say there will be no more frames.
//
// The client used to name the generated gRPC stream type directly. It never used more than these
// three methods of it, and narrowing to them costs nothing - proxyv1.ProxyService_SessionClient
// satisfies this as it stands - while making the transport substitutable.
//
// That matters because the protocol's frames are not tied to gRPC at all. The same serialised
// ClientMessage and ProxyMessage bytes can cross a C ABI into an embedded engine (see ../ffi), so
// an implementation of these three methods is the entire distance between "spawn a sidecar and
// dial it" and "link the engine into this process".
type sessionTransport interface {
	Send(*proxyv1.ClientMessage) error
	Recv() (*proxyv1.ProxyMessage, error)
	CloseSend() error
}
