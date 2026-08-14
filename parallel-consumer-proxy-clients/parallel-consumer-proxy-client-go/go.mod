// Copyright (C) 2026 Antony Stubbs and contributors
//
// The Go client of the language-proxy plan (astubbs#242). The pom.xml beside this file wraps the
// module into the Maven reactor under -Dpc.foreignClients.
//
// The `tool` directives below pin the protobuf/gRPC code generators BY VERSION rather than at
// @latest, so regenerating the committed stubs under gen/ is reproducible; scripts/generate-proto.sh
// builds them from these pins and never installs anything globally.

module github.com/astubbs/parallel-consumer/parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go

go 1.24

require (
	google.golang.org/grpc v1.71.1
	google.golang.org/protobuf v1.36.6
)

require (
	golang.org/x/net v0.34.0 // indirect
	golang.org/x/sys v0.29.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250115164207-1a7da9e5054f // indirect
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.5.1 // indirect
)

tool (
	google.golang.org/grpc/cmd/protoc-gen-go-grpc
	google.golang.org/protobuf/cmd/protoc-gen-go
)
