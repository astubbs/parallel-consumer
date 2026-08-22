// Copyright (C) 2026 Antony Stubbs and contributors

//go:build !pcffi

package main

// The ordinary build: sidecar only, no cgo, no native library needed. See embedded_arm_on.go.
const embeddedArmEnabled = false
