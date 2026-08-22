// Copyright (C) 2026 Antony Stubbs and contributors
//
// Its own module, separate from both bench/llingr and parallel-consumer-proxy-client-go. Benchmark
// code is not product, and a shipped artifact should never acquire a benchmark's dependencies
// transitively - the same rule bench/llingr follows for a much harder reason (AGPL). franz-go is
// BSD-3-Clause and nothing here links llingr code, so this directory carries no publication
// restriction: it is an ordinary control arm.
module bench/franz

go 1.25.0

require github.com/twmb/franz-go v1.21.5

require (
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
)
