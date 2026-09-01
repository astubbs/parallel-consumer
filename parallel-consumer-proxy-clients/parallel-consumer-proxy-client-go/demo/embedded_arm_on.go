// Copyright (C) 2026 Antony Stubbs and contributors

//go:build pcffi

package main

// Built with -tags pcffi, so the client has its embedded transport and the shared library is
// linked in. The arm is decided by the BUILD rather than by a flag because a flag could be set on
// a binary that cannot honour it, and the demo would then fail in the middle of a run instead of
// not offering the arm at all.
const embeddedArmEnabled = true
