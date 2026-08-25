// Copyright (C) 2026 Antony Stubbs and contributors
//
// Crossing-cost ladder arm (f): the fold compiled to wasm32. Rust-compiled, not
// C-compiled - this box has no clang/wasm-ld/emcc, and rustc with the
// wasm32-unknown-unknown target is the reachable to-WASM toolchain (recorded in
// docs/inflight/perf-crossing-cost-ladder.md). Exports: buf_ptr (a 4KB scratch buffer
// the host stages bytes in), noop, fold, and fold_spin (fold + caller-calibrated serial
// spin chain, the ~1us instrument check).
//
// Build: rustc --target wasm32-unknown-unknown -C opt-level=3 -C panic=abort \
//        --crate-type=cdylib fold_wasm.rs -o fold.wasm
#![no_std]

#[panic_handler]
fn panic(_: &core::panic::PanicInfo) -> ! {
    loop {}
}

static mut BUF: [u8; 4096] = [0; 4096];

#[no_mangle]
pub extern "C" fn buf_ptr() -> i32 {
    unsafe { BUF.as_ptr() as i32 }
}

#[no_mangle]
pub extern "C" fn noop(_k: i32, _klen: i32, _v: i32, _vlen: i32, _a: i32, _alen: i32) -> i32 {
    0
}

#[no_mangle]
pub extern "C" fn fold(k: i32, klen: i32, v: i32, vlen: i32, a: i32, alen: i32) -> i32 {
    unsafe {
        let n = if vlen < alen { vlen } else { alen };
        let key = k as *const u8;
        let val = v as *const u8;
        let acc = a as *mut u8;
        let kb = if klen > 0 { *key } else { 0 };
        let mut i: isize = 0;
        while i < n as isize {
            *acc.offset(i) = (*acc.offset(i)).wrapping_add(*val.offset(i)).wrapping_add(kb);
            i += 1;
        }
        if n > 0 { *acc.offset(n as isize - 1) as i32 } else { 0 }
    }
}

/// fold + a serial data-dependent chain of `count` steps written to observable memory -
/// the instrument-check injection, count calibrated to ~1us by the host.
#[no_mangle]
pub extern "C" fn fold_spin(k: i32, klen: i32, v: i32, vlen: i32, a: i32, alen: i32, count: i32) -> i32 {
    let r = fold(k, klen, v, vlen, a, alen);
    unsafe {
        let acc = a as *mut u8;
        let mut s = *acc as i32;
        let mut i = 0;
        while i < count {
            s = (s * 31 + i) & 0xFF;
            i += 1;
        }
        *acc = s as u8;
    }
    r
}
