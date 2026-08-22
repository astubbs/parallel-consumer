package main

/*
#cgo CFLAGS: -I${SRCDIR}/../build
#cgo LDFLAGS: -L${SRCDIR}/../build -lpcffi -Wl,-rpath,${SRCDIR}/../build
#include <libpcffi.h>
#include <graal_isolate.h>
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"os"
)

func main() {
	// Probe 0, check 2: can a foreign runtime create the isolate and attach a thread at all?
	var isolate *C.graal_isolate_t
	var thread *C.graal_isolatethread_t
	if rc := C.graal_create_isolate(nil, &isolate, &thread); rc != 0 {
		fmt.Printf("FAIL graal_create_isolate rc=%d\n", int(rc))
		os.Exit(1)
	}
	fmt.Println("ok   isolate created and thread attached from Go")

	// Check 1: the C ABI itself. If this is not 7 nothing below is meaningful.
	if got := int(C.pc_sum(thread, 3, 4)); got != 7 {
		fmt.Printf("FAIL pc_sum = %d, want 7\n", got)
		os.Exit(1)
	}
	fmt.Println("ok   pc_sum(3,4) = 7 - the C ABI works")

	// Is Parallel Consumer actually INSIDE the library? Both routes to the same fact.
	refl := int(C.pc_ordering_modes(thread))
	stat := int(C.pc_static_ordering_modes(thread))
	fmt.Printf("     ProcessingOrder constants: reflective=%d static=%d\n", refl, stat)
	if stat <= 0 {
		fmt.Println("FAIL PC's own classes are not linked into the shared library")
		os.Exit(1)
	}
	if refl != stat {
		fmt.Printf("NOTE reflective and static disagree (%d vs %d) - reflection was not auto-registered\n", refl, stat)
	}
	fmt.Println("ok   Parallel Consumer's own enum read from inside the Go process")
}
