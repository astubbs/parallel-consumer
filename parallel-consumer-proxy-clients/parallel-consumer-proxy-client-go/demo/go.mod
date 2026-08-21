// Copyright (C) 2026 Antony Stubbs and contributors
//
// A MODULE OF ITS OWN, AND THAT IS THE POINT. The demo needs a Kafka client for its AK core arm;
// the client LIBRARY must not. Go's module graph propagates requirements to every consumer, so
// putting franz-go in the library's go.mod would hand a Kafka client library to applications whose
// whole reason for using the proxy is not needing one. A nested module keeps that dependency here.
//
// The side effect is worth knowing: `go build ./...` and `go test ./...` in the parent module do
// NOT descend into a directory that has its own go.mod, so the parent's Maven-driven build never
// compiles this. demo/run.sh and the Dockerfile build it, and both entry points are exercised.
//
// The replace directive is what makes the demo drive THIS checkout's client library rather than a
// published version - there is none, and the demo must exercise the code beside it.

module github.com/astubbs/parallel-consumer/parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/demo

go 1.25.0

require (
	github.com/astubbs/parallel-consumer/parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go v0.0.0
	github.com/twmb/franz-go v1.21.5
	github.com/twmb/franz-go/pkg/kadm v1.18.0
)

require (
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
	golang.org/x/crypto v0.55.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
	google.golang.org/grpc v1.82.1 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/astubbs/parallel-consumer/parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go => ../
