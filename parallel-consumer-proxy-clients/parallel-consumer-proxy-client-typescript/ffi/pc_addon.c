/*
 * Copyright (C) 2026 Antony Stubbs and contributors
 *
 * A minimal N-API binding to the embedded engine's C ABI.
 *
 * WHY THIS EXISTS RATHER THAN AN FFI LIBRARY. koffi executes foreign calls on a stack it allocates
 * itself - which is why it has a configurable sync_stack_size at all. GraalVM derives its stack
 * guard zones from the calling OS thread's real stack, so a swapped stack makes those zones
 * inconsistent and the very first call dies with a fatal StackOverflowError. Raising koffi's stack
 * to its 16 MiB maximum changes nothing, because size was never the problem.
 *
 * An N-API addon calls straight down the thread's own stack, which is what ctypes and cgo do.
 *
 * Pointers cross as BigInt. Every entry point takes the IsolateThread for the CALLING thread and
 * never a cached one: a GraalVM isolate thread belongs to the OS thread it was attached on.
 */

#include <node_api.h>
#include <stdint.h>
#include <stdlib.h>

typedef void graal_isolate_t;
typedef void graal_isolatethread_t;

extern int graal_create_isolate(void *params, graal_isolate_t **isolate,
                                graal_isolatethread_t **thread);
extern int graal_attach_thread(graal_isolate_t *isolate, graal_isolatethread_t **thread);
extern graal_isolatethread_t *graal_get_current_thread(graal_isolate_t *isolate);

extern long long pc_session_open(graal_isolatethread_t *);
extern int pc_send(graal_isolatethread_t *, long long, char *, int);
extern int pc_next(graal_isolatethread_t *, long long, char *, int, int *, int);
extern int pc_session_close(graal_isolatethread_t *, long long);
extern int pc_last_error(graal_isolatethread_t *, long long, char *, int);

static uint64_t arg_u64(napi_env env, napi_value v) {
    uint64_t out = 0;
    bool lossless = false;
    napi_get_value_bigint_uint64(env, v, &out, &lossless);
    return out;
}

static napi_value make_u64(napi_env env, uint64_t value) {
    napi_value out;
    napi_create_bigint_uint64(env, value, &out);
    return out;
}

/** createIsolate() -> { isolate, thread } for this thread. */
static napi_value CreateIsolate(napi_env env, napi_callback_info info) {
    graal_isolate_t *isolate = NULL;
    graal_isolatethread_t *thread = NULL;
    int rc = graal_create_isolate(NULL, &isolate, &thread);
    napi_value result;
    napi_create_object(env, &result);
    napi_value rcv;
    napi_create_int32(env, rc, &rcv);
    napi_set_named_property(env, result, "rc", rcv);
    napi_set_named_property(env, result, "isolate", make_u64(env, (uint64_t) isolate));
    napi_set_named_property(env, result, "thread", make_u64(env, (uint64_t) thread));
    return result;
}

/** attachThread(isolate) -> thread for THIS OS thread. Idempotent. */
static napi_value AttachThread(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    graal_isolate_t *isolate = (graal_isolate_t *) arg_u64(env, argv[0]);

    graal_isolatethread_t *thread = graal_get_current_thread(isolate);
    if (thread == NULL) {
        if (graal_attach_thread(isolate, &thread) != 0) {
            napi_throw_error(env, NULL, "graal_attach_thread failed");
            return NULL;
        }
    }
    return make_u64(env, (uint64_t) thread);
}

static napi_value SessionOpen(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    long long handle = pc_session_open((graal_isolatethread_t *) arg_u64(env, argv[0]));
    return make_u64(env, (uint64_t) handle);
}

/** send(thread, handle, buffer) -> rc */
static napi_value Send(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    void *data = NULL;
    size_t length = 0;
    napi_get_buffer_info(env, argv[2], &data, &length);
    int rc = pc_send((graal_isolatethread_t *) arg_u64(env, argv[0]),
                     (long long) arg_u64(env, argv[1]), (char *) data, (int) length);
    napi_value out;
    napi_create_int32(env, rc, &out);
    return out;
}

/** next(thread, handle, buffer, timeoutMillis) -> { rc, length }. BLOCKS the calling thread. */
static napi_value Next(napi_env env, napi_callback_info info) {
    size_t argc = 4;
    napi_value argv[4];
    napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    void *data = NULL;
    size_t capacity = 0;
    napi_get_buffer_info(env, argv[2], &data, &capacity);
    int32_t timeout = 0;
    napi_get_value_int32(env, argv[3], &timeout);

    int written = 0;
    int rc = pc_next((graal_isolatethread_t *) arg_u64(env, argv[0]),
                     (long long) arg_u64(env, argv[1]), (char *) data, (int) capacity,
                     &written, timeout);

    napi_value result, rcv, lenv;
    napi_create_object(env, &result);
    napi_create_int32(env, rc, &rcv);
    napi_create_int32(env, written, &lenv);
    napi_set_named_property(env, result, "rc", rcv);
    napi_set_named_property(env, result, "length", lenv);
    return result;
}

static napi_value SessionClose(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    int rc = pc_session_close((graal_isolatethread_t *) arg_u64(env, argv[0]),
                              (long long) arg_u64(env, argv[1]));
    napi_value out;
    napi_create_int32(env, rc, &out);
    return out;
}

#define EXPORT(name, fn)                                                    \
    do {                                                                    \
        napi_value _f;                                                      \
        napi_create_function(env, name, NAPI_AUTO_LENGTH, fn, NULL, &_f);   \
        napi_set_named_property(env, exports, name, _f);                    \
    } while (0)

static napi_value Init(napi_env env, napi_value exports) {
    EXPORT("createIsolate", CreateIsolate);
    EXPORT("attachThread", AttachThread);
    EXPORT("sessionOpen", SessionOpen);
    EXPORT("send", Send);
    EXPORT("next", Next);
    EXPORT("sessionClose", SessionClose);
    return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, Init)
