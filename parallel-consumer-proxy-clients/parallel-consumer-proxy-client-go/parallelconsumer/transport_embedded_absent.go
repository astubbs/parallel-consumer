// Copyright (C) 2026 Antony Stubbs and contributors

//go:build !pcffi

package parallelconsumer

import (
	"context"
	"errors"
)

// The default build has no embedded engine, and says so rather than quietly using the sidecar.
// Keeping this behind a build tag is what lets the library stay pure Go: an application that does
// not ask for the embedded engine needs neither cgo nor a native library on disk.
func openEmbedded(context.Context, Options) (*Client, error) {
	return nil, errors.New("parallelconsumer: Options.Embedded needs a build with -tags pcffi, " +
		"and the shared library from ffi/build-shared-library.sh")
}
