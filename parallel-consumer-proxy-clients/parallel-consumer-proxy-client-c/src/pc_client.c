/*
 * Copyright (C) 2026 Antony Stubbs and contributors
 *
 * Parallel Consumer, consumed from C. No gRPC, no JVM, no sidecar process.
 *
 * THIS IS THE REACH PROBE, and it is the one case none of the other clients can test. All eleven
 * of them already have gRPC; the argument for a C ABI was always about the languages that do not -
 * embedded targets, and runtimes with no gRPC stack at all. C is what every one of those binds
 * through, so if C works the surface is genuinely language-neutral rather than three lucky fits.
 *
 * The parked note raised two objections to a C client. The first - "there is no pure-C gRPC" -
 * simply does not apply here, because there is no gRPC anywhere on this path. The second - "no
 * official C protobuf, only third-party" - is the real one, and this file is the answer to it.
 *
 * protobuf-c could not compile the protocol AT ALL: it rejects proto3 `optional`, which this
 * protocol uses in 42 places because absence has to be distinguishable from zero. nanopb accepts
 * it, and with field bounds in proxy.options every field becomes a plain C struct member - no
 * callbacks, no malloc, a message whose size is known at compile time.
 */

#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <pb_decode.h>
#include <pb_encode.h>

#include "parallelconsumer/proxy/v1/proxy.pb.h"

typedef void graal_isolate_t;
typedef void graal_isolatethread_t;

extern int graal_create_isolate(void *params, graal_isolate_t **isolate,
                                graal_isolatethread_t **thread);
extern long long pc_session_open(graal_isolatethread_t *);
extern int pc_send(graal_isolatethread_t *, long long, char *, int);
extern int pc_next(graal_isolatethread_t *, long long, char *, int, int *, int);
extern int pc_session_close(graal_isolatethread_t *, long long);
extern int pc_last_error(graal_isolatethread_t *, long long, char *, int);

#define PC_OK 0
#define PC_ERR_TIMEOUT (-3)
#define PC_ERR_SESSION_ENDED (-4)

#define FRAME_CAPACITY 65536
#define MAX_KEYS 4096
#define KEY_LENGTH 64

static const char *code_name(int code) {
    switch (code) {
        case 0:  return "OK";
        case -1: return "ERR_NO_SESSION";
        case -2: return "ERR_BUFFER_TOO_SMALL";
        case -3: return "ERR_TIMEOUT";
        case -4: return "ERR_SESSION_ENDED";
        case -5: return "ERR_BAD_FRAME";
        case -6: return "ERR_INTERNAL";
        default: return "unknown";
    }
}

/* Distinct-key tally. Linear scan over a fixed array: the demo's key space is small, and the point
 * here is to avoid a hash table rather than to be clever - no allocation anywhere in this file. */
static char seen_keys[MAX_KEYS][KEY_LENGTH];
static size_t seen_key_count = 0;

static void observe_key(const uint8_t *key, size_t length) {
    if (length == 0 || length >= KEY_LENGTH) return;
    for (size_t i = 0; i < seen_key_count; i++) {
        if (strncmp(seen_keys[i], (const char *) key, length) == 0
            && seen_keys[i][length] == '\0') {
            return;
        }
    }
    if (seen_key_count >= MAX_KEYS) return;
    memcpy(seen_keys[seen_key_count], key, length);
    seen_keys[seen_key_count][length] = '\0';
    seen_key_count++;
}

static void put_property(parallelconsumer_proxy_v1_Configure *configure,
                         const char *key, const char *value) {
    pb_size_t i = configure->kafka_properties_count;
    snprintf(configure->kafka_properties[i].key, sizeof configure->kafka_properties[i].key,
             "%s", key);
    snprintf(configure->kafka_properties[i].value, sizeof configure->kafka_properties[i].value,
             "%s", value);
    configure->kafka_properties_count = i + 1;
}

static bool send_message(graal_isolatethread_t *thread, long long session,
                         const parallelconsumer_proxy_v1_ClientMessage *message) {
    uint8_t frame[FRAME_CAPACITY];
    pb_ostream_t out = pb_ostream_from_buffer(frame, sizeof frame);
    if (!pb_encode(&out, parallelconsumer_proxy_v1_ClientMessage_fields, message)) {
        fprintf(stderr, "FAIL encoding a ClientMessage: %s\n", PB_GET_ERROR(&out));
        return false;
    }
    int rc = pc_send(thread, session, (char *) frame, (int) out.bytes_written);
    if (rc != PC_OK) {
        fprintf(stderr, "FAIL pc_send returned %s\n", code_name(rc));
        return false;
    }
    return true;
}

/* Pulls one frame, retrying while the session is merely idle. A timeout is not an ending. */
static bool next_message(graal_isolatethread_t *thread, long long session,
                         parallelconsumer_proxy_v1_ProxyMessage *message,
                         int timeout_millis, int *rc_out) {
    uint8_t frame[FRAME_CAPACITY];
    for (;;) {
        int written = 0;
        int rc = pc_next(thread, session, (char *) frame, (int) sizeof frame, &written,
                         timeout_millis);
        *rc_out = rc;
        if (rc == PC_ERR_TIMEOUT) return false;
        if (rc != PC_OK) return false;

        pb_istream_t in = pb_istream_from_buffer(frame, (size_t) written);
        *message = (parallelconsumer_proxy_v1_ProxyMessage) parallelconsumer_proxy_v1_ProxyMessage_init_zero;
        if (!pb_decode(&in, parallelconsumer_proxy_v1_ProxyMessage_fields, message)) {
            fprintf(stderr, "FAIL decoding a ProxyMessage: %s\n", PB_GET_ERROR(&in));
            *rc_out = -100;
            return false;
        }
        return true;
    }
}

static const char *env_or(const char *name, const char *fallback) {
    const char *value = getenv(name);
    return (value != NULL && value[0] != '\0') ? value : fallback;
}

int main(void) {
    const char *broker = env_or("PC_BROKER", "localhost:19092");
    const char *topic = env_or("PC_TOPIC", "pc-ffi-demo");
    const int target = atoi(env_or("PC_EXPECT", "200"));

    graal_isolate_t *isolate = NULL;
    graal_isolatethread_t *thread = NULL;
    if (graal_create_isolate(NULL, &isolate, &thread) != 0) {
        fprintf(stderr, "FAIL graal_create_isolate\n");
        return 1;
    }
    printf("ok   isolate created from a C process\n");

    long long session = pc_session_open(thread);
    if (session <= 0) {
        fprintf(stderr, "FAIL pc_session_open returned %s\n", code_name((int) session));
        return 1;
    }
    printf("ok   pc_session_open -> %lld\n", session);

    /* ---- the handshake ------------------------------------------------------------------- */
    parallelconsumer_proxy_v1_ClientMessage out = parallelconsumer_proxy_v1_ClientMessage_init_zero;
    out.which_message = parallelconsumer_proxy_v1_ClientMessage_configure_tag;
    parallelconsumer_proxy_v1_Configure *configure = &out.message.configure;

    configure->topics_count = 1;
    snprintf(configure->topics[0], sizeof configure->topics[0], "%s", topic);
    configure->has_max_concurrency = true;
    configure->max_concurrency = 16;
    configure->has_ordering = true;
    configure->ordering = parallelconsumer_proxy_v1_ProcessingOrder_PROCESSING_ORDER_UNORDERED;
    configure->capabilities_count = 1;
    snprintf(configure->capabilities[0], sizeof configure->capabilities[0], "dispatch");

    char group[128];
    snprintf(group, sizeof group, "pc-c-embedded-%ld", (long) time(NULL));
    put_property(configure, "bootstrap.servers", broker);
    put_property(configure, "group.id", group);
    put_property(configure, "auto.offset.reset", "earliest");

    if (!send_message(thread, session, &out)) return 1;
    printf("ok   Configure sent (broker=%s topic=%s)\n", broker, topic);

    parallelconsumer_proxy_v1_ProxyMessage in;
    int rc = 0;
    if (!next_message(thread, session, &in, 30000, &rc)) {
        fprintf(stderr, "FAIL awaiting Configured: %s\n", code_name(rc));
        return 1;
    }
    if (in.which_message != parallelconsumer_proxy_v1_ProxyMessage_configured_tag) {
        fprintf(stderr, "FAIL the handshake reply was tag %d, not Configured\n", in.which_message);
        return 1;
    }
    printf("ok   Configured: max_concurrency=%" PRId32 " executor_count=%" PRId32 "\n",
           in.message.configured.max_concurrency, in.message.configured.executor_count);

    /* ---- the dispatch loop --------------------------------------------------------------- */
    int processed = 0;
    const time_t deadline = time(NULL) + 120;
    while (processed < target && time(NULL) < deadline) {
        if (!next_message(thread, session, &in, 500, &rc)) {
            if (rc == PC_ERR_TIMEOUT) continue;          /* idle, not ended */
            if (rc == PC_ERR_SESSION_ENDED) break;
            fprintf(stderr, "FAIL pc_next returned %s\n", code_name(rc));
            return 1;
        }
        if (in.which_message != parallelconsumer_proxy_v1_ProxyMessage_dispatch_tag) continue;

        const parallelconsumer_proxy_v1_Dispatch *dispatch = &in.message.dispatch;
        for (pb_size_t i = 0; i < dispatch->records_count; i++) {
            const parallelconsumer_proxy_v1_DispatchRecord *dispatched = &dispatch->records[i];

            /* PLACE YOUR PROCESSING HERE. This probe only counts, because records and distinct
             * keys are the deterministic pair the seeding predicts - a rate is not. */
            if (dispatched->has_record && dispatched->record.has_key) {
                observe_key(dispatched->record.key.bytes, dispatched->record.key.size);
            }
            processed++;

            parallelconsumer_proxy_v1_ClientMessage verdict =
                parallelconsumer_proxy_v1_ClientMessage_init_zero;
            verdict.which_message = parallelconsumer_proxy_v1_ClientMessage_report_tag;
            verdict.message.report.has_token = dispatched->has_token;
            verdict.message.report.token = dispatched->token;
            verdict.message.report.which_outcome = parallelconsumer_proxy_v1_Report_success_tag;
            verdict.message.report.outcome.success.produce_count = 0;
            if (!send_message(thread, session, &verdict)) return 1;
        }
    }

    pc_session_close(thread, session);

    printf("\n  %d records over %zu keys\n", processed, seen_key_count);
    if (processed < target) {
        fprintf(stderr, "FAIL ended at %d of %d\n", processed, target);
        return 1;
    }
    printf("\nPARALLEL CONSUMER RAN INSIDE THIS C PROCESS - no sidecar, no gRPC, no JVM\n");
    return 0;
}
