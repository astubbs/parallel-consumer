// Copyright (C) 2026 Antony Stubbs and contributors
//
// Its own module, deliberately kept out of the repo's other Go module: the dependency below is
// AGPL-3.0, and nothing that ships from this repository may acquire it transitively. See NOTICE.md
// in this directory before building.
module bench/llingr

go 1.25.0

require (
	github.com/llingr/llingr-adapter-franz v0.14.0
	github.com/llingr/llingr-demux v0.12.2
	github.com/llingr/llingr-nexus v0.11.0
	github.com/twmb/franz-go v1.21.5
	github.com/twmb/franz-go/pkg/kadm v1.18.0
)

require (
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
	golang.org/x/crypto v0.51.0 // indirect
)
