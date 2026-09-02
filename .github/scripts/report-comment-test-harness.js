// Copyright (C) 2026 Antony Stubbs and contributors
//
// THE TEST HARNESS THE THREE REPORT-COMMENT SUITES SHARE.
//
// Three suites arrived in one change - sticky, throughput, quarantine - each carrying its own copy
// of the same tiny runner and the same fake GitHub client. Two of the copies were byte-identical.
// That is the duplication this repo's own rule catches at review time, so it is collapsed here
// rather than left for the clone detector to report on the PR that introduced it.
//
// CommonJS on purpose: these suites test `github-script` modules, which require() rather than
// import, and the .github/scripts/ half of this repo follows that. bin/*.mjs being ESM is the
// deliberate other half, not an inconsistency.
//
// THE FAKE IS THE RICHEST OF THE THREE, NOT THE SMALLEST COMMON ONE. sticky's version recorded
// call ORDER and could simulate a failing create or forward-link; the other two recorded neither.
// Sharing down to the simplest would have silently deleted the only coverage of retire-before-
// create - a property no assertion about final state can see - so the superset is what ships and
// the simpler callers just pass fewer options.

/**
 * A test runner scoped to one suite.
 *
 * Returned rather than module-level, because module state shared across two suites in one process
 * makes each suite's failure count depend on what ran before it.
 */
function makeRunner() {
    let failures = 0;
    const pending = [];
    const record = (name, error) => {
        if (!error) return console.log(`  ok  ${name}`);
        console.log(`FAIL  ${name}\n      ${error.message.replace(/\n/g, "\n      ")}`);
        failures++;
    };
    return {
        // EVERYTHING IS QUEUED, INCLUDING SECTION HEADINGS. Half these tests are async, and running
        // the sync ones eagerly printed them all before the first `await` resolved - so every async
        // result landed under the LAST heading in the file and the sections between printed empty.
        // A report that files results under the wrong heading is the same defect class as the code
        // it tests.
        section: name => pending.push(() => console.log(name)),
        test: (name, fn) => pending.push(() => {
            try {
                const result = fn();
                // AN ASYNC BODY HANDED TO THE SYNC test() CAN NEVER FAIL: the assertion rejects
                // after record() already logged `ok`. Only one of the three suites had this guard;
                // sharing the runner is what gives it to the other two.
                if (result && typeof result.then === "function") {
                    throw new Error("async test body passed to test() - use asyncTest, or it can never fail");
                }
                record(name);
            } catch (e) { record(name, e); }
        }),
        asyncTest: (name, fn) => pending.push(async () => { try { await fn(); record(name); } catch (e) { record(name, e); } }),
        /** Runs everything queued, reports, and sets the process exit code. */
        runAll: async () => {
            for (const r of pending) await r();
            console.log(failures ? `\n${failures} test(s) failed` : "\nAll tests passed");
            process.exit(failures ? 1 : 0);
        },
    };
}

/**
 * A fake GitHub client that records every call, IN ORDER.
 *
 * The order is the point for half of these tests: retire-before-create is invisible to any
 * assertion that only inspects final state.
 */
function fakeGithub({ comments = [], failCreate = false, failForwardLink = false } = {}) {
    const calls = [];
    let nextId = 900;
    const store = comments.map(c => ({ ...c }));
    let created = null;
    return {
        calls,
        store,
        paginate: async (fn, params) => {
            calls.push({ op: "paginate", params });
            return fn(params);
        },
        rest: {
            issues: {
                listComments: async params => {
                    calls.push({ op: "listComments", params });
                    return store;
                },
                updateComment: async ({ comment_id, body }) => {
                    calls.push({ op: "updateComment", comment_id, body });
                    if (failForwardLink && created && body.includes("Superseded by")) {
                        throw new Error("simulated forward-link failure");
                    }
                    const target = store.find(c => c.id === comment_id);
                    if (target) target.body = body;
                    return { data: { id: comment_id, body } };
                },
                createComment: async ({ body }) => {
                    calls.push({ op: "createComment", body });
                    if (failCreate) throw new Error("simulated create failure");
                    created = { id: ++nextId, body, html_url: `https://example.test/c/${nextId}` };
                    store.push({ ...created, user: { type: "Bot" } });
                    return { data: created };
                },
            },
        },
    };
}

/** The github-script `context` shape these reports read. Override any field per test. */
const fakeContext = (overrides = {}) => ({
    repo: { owner: "astubbs", repo: "parallel-consumer" },
    issue: { number: 29 },
    serverUrl: "https://github.com",
    runId: 7,
    payload: { pull_request: { number: 29, head: { sha: "abcdef1234567890" } } },
    ...overrides,
});

/** A `core` that REMEMBERS its warnings, so a test can assert one was emitted rather than ignored. */
const fakeCore = () => {
    const warnings = [];
    return { warnings, warning: m => warnings.push(m) };
};

module.exports = { makeRunner, fakeGithub, fakeContext, fakeCore };
