// Copyright (C) 2026 Antony Stubbs and contributors
//
// The Go client of the language-proxy plan (astubbs#242). The pom.xml beside this file wraps the
// module into the Maven reactor under -Dpc.foreignClients.
//
// The `tool` directives below pin the protobuf/gRPC code generators BY VERSION rather than at
// @latest, so regenerating the committed stubs under gen/ is reproducible; scripts/generate-proto.sh
// builds them from these pins and never installs anything globally.
//
// staticcheck is pinned the same way and for the same reason - `go tool staticcheck` builds the
// version named here, so the analyser a developer runs and the one the CI row runs cannot drift,
// and neither needs a global install. scripts/analyse.sh is its entry point; docs/client-static-
// analysis.md carries the per-language policy. Adding it moved the `go` directive to 1.25.0, which
// is staticcheck v0.7.0's floor and matches the toolchain the clients workflow pins for this row.

module github.com/astubbs/parallel-consumer/parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go

go 1.25.0

require (
	google.golang.org/grpc v1.71.1
	google.golang.org/protobuf v1.36.6
)

require (
	github.com/BurntSushi/toml v1.4.1-0.20240526193622-a339e1f7089c // indirect
	golang.org/x/exp/typeparams v0.0.0-20231108232855-2478ac86f678 // indirect
	golang.org/x/mod v0.31.0 // indirect
	golang.org/x/net v0.48.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/text v0.32.0 // indirect
	golang.org/x/tools v0.40.1-0.20260108161641-ca281cf95054 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250115164207-1a7da9e5054f // indirect
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.5.1 // indirect
	honnef.co/go/tools v0.7.0 // indirect
)

tool (
	google.golang.org/grpc/cmd/protoc-gen-go-grpc
	google.golang.org/protobuf/cmd/protoc-gen-go
	honnef.co/go/tools/cmd/staticcheck
)
