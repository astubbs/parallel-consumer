/*
 * Copyright (C) 2026 Antony Stubbs and contributors
 *
 * Crossing-cost ladder (docs/inflight/perf-crossing-cost-ladder.md): the shared C side.
 * Provides the no-op, the windowed-aggregation-shaped fold (fold ~1KB value into ~1KB
 * accumulator), the ~1us busy-wait instrument-check variant, and a C-side driver that
 * calls an arbitrary function pointer N times with timing done in C - the proxy for
 * what the engine pays to call a registered pointer (arms c' and d).
 *
 * Build: cc -O2 -shared -fPIC fold.c -o libfold.so
 */
#include <stdint.h>
#include <time.h>

static inline uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t) ts.tv_sec * 1000000000ull + (uint64_t) ts.tv_nsec;
}

/* the no-op crossing target */
int32_t pc_noop(const uint8_t *key, int32_t klen, const uint8_t *val, int32_t vlen,
                uint8_t *acc, int32_t alen) {
    (void) key; (void) klen; (void) val; (void) vlen; (void) acc; (void) alen;
    return 0;
}

/* the fold: element-wise add value into accumulator, seasoned by the key's first byte */
int32_t pc_fold(const uint8_t *key, int32_t klen, const uint8_t *val, int32_t vlen,
                uint8_t *acc, int32_t alen) {
    int32_t n = vlen < alen ? vlen : alen;
    uint8_t k = klen > 0 ? key[0] : 0;
    for (int32_t i = 0; i < n; i++) {
        acc[i] = (uint8_t) (acc[i] + val[i] + k);
    }
    return acc[n > 0 ? n - 1 : 0];
}

/* ~1us busy-wait, clock-calibrated - the instrument-check injection */
void pc_spin_1us(void) {
    uint64_t start = now_ns();
    while (now_ns() - start < 1000ull) { /* spin */ }
}

int32_t pc_fold_spin(const uint8_t *key, int32_t klen, const uint8_t *val, int32_t vlen,
                     uint8_t *acc, int32_t alen) {
    int32_t r = pc_fold(key, klen, val, vlen, acc, alen);
    pc_spin_1us();
    return r;
}

/* engine-side proxy: call fn `iters` times, return elapsed ns measured in C.
 * `sink` prevents the calls being optimised out (the fn is behind a pointer anyway). */
typedef int32_t (*fold_fn)(const uint8_t *, int32_t, const uint8_t *, int32_t, uint8_t *, int32_t);

uint64_t pc_drive_ptr(fold_fn fn, uint64_t iters,
                      const uint8_t *key, int32_t klen, const uint8_t *val, int32_t vlen,
                      uint8_t *acc, int32_t alen, int32_t *sink) {
    volatile int32_t s = 0;
    uint64_t start = now_ns();
    for (uint64_t i = 0; i < iters; i++) {
        s += fn(key, klen, val, vlen, acc, alen);
    }
    uint64_t elapsed = now_ns() - start;
    *sink = s;
    return elapsed;
}
