/*
 * Copyright (C) 2026 Antony Stubbs and contributors
 *
 * The page shell and the live loop.
 *
 * THE ONE SEAM THAT MATTERS. Nothing renders from a fetch callback or an event handler. Documents arrive and are
 * parked; a requestAnimationFrame loop builds an INTERPOLATED view from the last two of them and hands that to the
 * panels. Smoothness therefore costs animation frames on this machine, never sampling rate against the running
 * consumer (plan R19, KTD7). Any panel that renders straight from an arrival breaks that property for the whole
 * page, so panels are only ever given the interpolated view.
 *
 * TRANSPORT. Server-sent events first, because they cost no round trip per tick and EventSource reconnects on its
 * own - a dropped stream is routine and is not reported as an error. Polling is the fallback for the case the
 * stream cannot cover: a proxy that buffers or strips event streams, or an instance already at its stream cap,
 * which answers 503 with Retry-After. Both stop when the tab is hidden.
 *
 * FOUR PAGE STATES, PLUS THE ONES THAT ARE NOT FAILURES. Loading, live, idle, stale and error are distinguished
 * explicitly, and "off" and "no meters" are their own states rather than being smuggled into one of those. Stale
 * must never look like idle: a chart quietly flatlining to zero is indistinguishable from real zero traffic, so
 * staleness is stated in words with an age beside it (plan R18).
 */

import {
    formatDuration,
    formatInstant,
    interpolateNumber,
    readPersisted,
    setAttribute,
    setText,
    toGeometryNumber,
    writePersisted
} from './ui.js';
import {isInterruption, nextGapEstimate, tweenFraction} from './tween.js';
import {createOffsetsPanel} from './panels/offsets.js';

/* ------------------------------------------------------------------ wiring */

const STATE_PATH = '/api/state.json';
const STREAM_PATH = '/api/stream';
const SNAPSHOT_EVENT = 'snapshot';

/** Set by StateRoute; lower-cased because the Fetch API's header lookup is case-insensitive but explicit is kinder. */
const UPDATE_INTERVAL_HEADER = 'x-pc-dashboard-update-interval-millis';

const SCHEMA_VERSION = 1;

const CADENCE_STORAGE_KEY = 'pc-dashboard.cadence';

/**
 * The floor on how often this page may ask the instance for state. A second is documented as causing problems in
 * comparable tools, and no operational question is answered faster by asking twice as often.
 */
const MINIMUM_POLL_MILLIS = 2000;

/** Used only until the server has told us its own cadence. */
const ASSUMED_INTERVAL_MILLIS = 5000;

/** A snapshot older than this many expected intervals is stale. Floored, so jitter cannot cry wolf. */
const STALE_INTERVAL_MULTIPLE = 3;

const MINIMUM_STALE_MILLIS = 8000;

const MAXIMUM_BACKOFF_MILLIS = 30000;

/** How many consecutive stream errors are tolerated before the page decides the stream is not going to work. */
const STREAM_ERROR_TOLERANCE = 3;

const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)');

const dom = {
    status: document.getElementById('status'),
    statusLabel: document.getElementById('status-label'),
    statusDetail: document.getElementById('status-detail'),
    statusAge: document.getElementById('status-age'),
    instanceId: document.getElementById('instance-id'),
    cadence: document.getElementById('cadence'),
    transport: document.getElementById('transport'),
    panels: document.getElementById('panels'),
    notice: document.getElementById('notice'),
    footerMeta: document.getElementById('footer-meta')
};

/* ------------------------------------------------------------------- state */

const feed = {
    /** What the server says its own sampling cadence is, in milliseconds. Null until it has told us. */
    advertisedIntervalMillis: null,
    /** 'stream', 'poll', 'off', 'hidden' or 'starting'. */
    transport: 'starting',
    /** Set once the stream has proved it cannot work here; polling then owns the session until the user retries. */
    streamBlocked: false,
    /** Why the stream was abandoned, for the transport chip's tooltip. */
    streamBlockedReason: null,
    /** The current transport failure, or null. Distinct from staleness: this is "the request failed". */
    errorText: null,
    /** { document, monotonicMillis, wallClockMillis } for the newest and the one before it. */
    current: null,
    previous: null,
    /** How long the next document is expected to take to arrive. See tween.js. Null until two have landed. */
    gapEstimateMillis: null,
    documentsReceived: 0
};

let eventSource = null;
let pollTimerId = null;
let pollAbortController = null;
let pollInFlight = false;
let consecutiveFailures = 0;
let consecutiveStreamErrors = 0;

/**
 * Set immediately before WE abort the in-flight poll, and cleared when the next poll starts.
 *
 * An abort raised by `stopTransport()` is a deliberate transport switch - a cadence change, a transport-retry click,
 * the tab being hidden - and reporting it as a network failure would be a lie AND a penalty: it would print "Could
 * not read /api/state.json" over a working instance and replace the immediate re-poll `startPolling()` just
 * scheduled with a multi-second backoff. Distinguishing it by `document.hidden` only covers the hidden-tab case, and
 * every visible-tab trigger falls through to the error path.
 */
let intentionalAbort = false;

/** Set when a new document arrives, so the render loop can idle once everything has settled. */
let dirty = true;
let renderedSettled = false;

const panels = [createOffsetsPanel()];

/* -------------------------------------------------------------- cadence UI */

function chosenCadence() {
    return dom.cadence.value;
}

/**
 * How often this page will ask, in milliseconds. "Live" means the server's own advertised cadence, floored - the
 * server is the thing that knows how often its state actually changes, so its number is the default rather than
 * one this page invented (plan U5 step 3b).
 */
function pollIntervalMillis() {
    const choice = chosenCadence();
    if (choice === 'live') {
        return Math.max(MINIMUM_POLL_MILLIS, feed.advertisedIntervalMillis || ASSUMED_INTERVAL_MILLIS);
    }
    const explicit = Number(choice);
    return Number.isFinite(explicit) ? Math.max(MINIMUM_POLL_MILLIS, explicit) : MINIMUM_POLL_MILLIS;
}

/**
 * How often a new snapshot is EXPECTED, which is what staleness is measured against. On the stream this is the
 * server's cadence rather than a poll period, because nothing here is doing the asking.
 */
function expectedIntervalMillis() {
    if (chosenCadence() === 'live') {
        return feed.advertisedIntervalMillis || ASSUMED_INTERVAL_MILLIS;
    }
    return pollIntervalMillis();
}

function staleThresholdMillis() {
    return Math.max(MINIMUM_STALE_MILLIS, STALE_INTERVAL_MULTIPLE * expectedIntervalMillis());
}

/* --------------------------------------------------------------- transport */

function stopTransport() {
    if (eventSource) {
        eventSource.close();
        eventSource = null;
    }
    if (pollTimerId !== null) {
        window.clearTimeout(pollTimerId);
        pollTimerId = null;
    }
    if (pollAbortController) {
        // flagged BEFORE the abort, because the rejection can be delivered synchronously
        intentionalAbort = true;
        pollAbortController.abort();
        pollAbortController = null;
    }
}

/**
 * Chooses and starts the transport for the current cadence and visibility. Called on every change of either, and
 * safe to call repeatedly - it always tears down first.
 */
function applyTransport() {
    stopTransport();

    if (document.hidden) {
        // R19's other half: a tab nobody is looking at must not keep asking a production instance for state
        feed.transport = 'hidden';
        return;
    }
    if (chosenCadence() === 'off') {
        feed.transport = 'off';
        return;
    }
    // A cadence slower than the server's own is a deliberate throttle, and the stream cannot honour it - the server
    // pushes at its cadence, not ours. So an explicit choice polls, and only "live" streams.
    if (chosenCadence() === 'live' && !feed.streamBlocked && typeof window.EventSource === 'function') {
        startStream();
    } else {
        startPolling();
    }
}

function startStream() {
    feed.transport = 'stream';
    consecutiveStreamErrors = 0;
    // Captured, and every listener checks it is still the current one. `close()` does not guarantee no further
    // event is dispatched for an already-queued one, and `applyTransport` can supersede a stream at any moment - so
    // without this guard a trailing event from an abandoned generation resets the live generation's error counter,
    // clears its error text, or trips its fall-back-to-polling on a connection nobody is using any more.
    const source = new EventSource(STREAM_PATH);
    eventSource = source;

    source.addEventListener(SNAPSHOT_EVENT, event => {
        if (source !== eventSource) {
            return;
        }
        consecutiveStreamErrors = 0;
        consecutiveFailures = 0;
        feed.errorText = null;
        acceptDocument(event.data, 'the event stream');
    });

    source.addEventListener('open', () => {
        if (source !== eventSource) {
            return;
        }
        consecutiveStreamErrors = 0;
        feed.errorText = null;
    });

    source.addEventListener('error', () => {
        if (source !== eventSource) {
            return;
        }
        if (source.readyState === EventSource.CLOSED) {
            // The browser only gives up permanently when the response was not an event stream at all - which is
            // exactly the 503-past-the-cap answer, and exactly what a proxy that strips the content type produces.
            fallBackToPolling('The instance would not open an event stream, so this page is polling instead. It '
                + 'answers 503 once it already has as many streams open as it is configured to allow.');
            return;
        }
        // Otherwise the connection merely dropped, which EventSource reconnects by itself. That is routine and is
        // deliberately NOT surfaced as an error - if it does not come back, staleness will say so on its own.
        consecutiveStreamErrors += 1;
        if (consecutiveStreamErrors >= STREAM_ERROR_TOLERANCE) {
            fallBackToPolling('The event stream kept dropping, so this page is polling instead.');
        }
    });
}

function fallBackToPolling(reason) {
    feed.streamBlocked = true;
    feed.streamBlockedReason = reason;
    applyTransport();
}

function startPolling() {
    feed.transport = 'poll';
    scheduleNextPoll(0);
}

function scheduleNextPoll(delayMillis) {
    if (pollTimerId !== null) {
        window.clearTimeout(pollTimerId);
    }
    pollTimerId = window.setTimeout(poll, delayMillis);
}

function backoffMillis() {
    const base = pollIntervalMillis();
    const scaled = base * Math.pow(2, Math.min(consecutiveFailures, 6));
    return Math.min(MAXIMUM_BACKOFF_MILLIS, scaled);
}

async function poll() {
    if (pollInFlight) {
        // a tick arriving while the last request is still out is dropped, not queued - a slow instance must not
        // accumulate a backlog of requests against itself
        scheduleNextPoll(pollIntervalMillis());
        return;
    }
    pollInFlight = true;
    intentionalAbort = false;
    pollAbortController = new AbortController();
    const abortTimerId = window.setTimeout(() => pollAbortController.abort(),
        Math.max(5000, pollIntervalMillis() * 2));
    try {
        const response = await fetch(STATE_PATH, {
            signal: pollAbortController.signal,
            headers: {Accept: 'application/json'},
            cache: 'no-cache'
        });
        if (!response.ok) {
            throw new Error('the instance answered ' + response.status + ' ' + response.statusText);
        }
        readAdvertisedInterval(response);
        acceptDocument(await response.text(), 'a poll of the state document');
        consecutiveFailures = 0;
        feed.errorText = null;
        scheduleNextPoll(pollIntervalMillis());
    } catch (failure) {
        if (failure && failure.name === 'AbortError' && intentionalAbort) {
            // WE aborted it, on the way to a different transport - not a failure of the instance, and the transport
            // we switched to has already scheduled its own first request. Reporting this would both invent a network
            // error and cancel that request in favour of a backoff. The timeout abort below is a real failure and
            // does NOT set the flag, so it still reports.
            return;
        }
        consecutiveFailures += 1;
        feed.errorText = 'Could not read ' + STATE_PATH + ' - ' + describeFailure(failure)
            + '. Retrying in ' + Math.round(backoffMillis() / 1000) + 's.';
        scheduleNextPoll(backoffMillis());
    } finally {
        window.clearTimeout(abortTimerId);
        pollInFlight = false;
        pollAbortController = null;
    }
}

function describeFailure(failure) {
    if (!failure) {
        return 'an unknown failure';
    }
    if (failure.name === 'AbortError') {
        return 'the request took longer than the poll interval and was abandoned';
    }
    return failure.message || String(failure);
}

function readAdvertisedInterval(response) {
    const raw = response.headers.get(UPDATE_INTERVAL_HEADER);
    const millis = Number(raw);
    if (Number.isFinite(millis) && millis > 0) {
        feed.advertisedIntervalMillis = millis;
    }
}

/**
 * Parks a newly-arrived document. Deliberately does NOT render: see the module header.
 */
function acceptDocument(payload, source) {
    let parsed;
    try {
        parsed = typeof payload === 'string' ? JSON.parse(payload) : payload;
    } catch (failure) {
        feed.errorText = 'The state document from ' + source + ' would not parse as JSON: ' + describeFailure(failure);
        dirty = true;
        return;
    }
    if (parsed && parsed.schemaVersion !== SCHEMA_VERSION) {
        feed.errorText = 'This page understands state document schema version ' + SCHEMA_VERSION + ', but the '
            + 'instance is serving version ' + parsed.schemaVersion + '. Reload to pick up the page that ships with '
            + 'it; what is drawn below may be wrong.';
    }
    const arrivedAt = performance.now();
    const gapMillis = feed.current === null ? null : arrivedAt - feed.current.monotonicMillis;
    // An interruption is not a cadence: dropping `previous` makes the next frame SNAP to the new sample instead of
    // gliding across a hole, and clearing the estimate stops the hole becoming the next tween's duration
    const interrupted = isInterruption(gapMillis, staleThresholdMillis());
    feed.gapEstimateMillis = interrupted ? null : nextGapEstimate(feed.gapEstimateMillis, gapMillis);
    feed.previous = interrupted ? null : feed.current;
    feed.current = {
        document: parsed,
        monotonicMillis: arrivedAt,
        wallClockMillis: Date.now()
    };
    feed.documentsReceived += 1;
    dirty = true;
    renderedSettled = false;
}

/* ------------------------------------------------------- interpolated view */

/**
 * Builds the view the panels render from: the newest document, with every numeric quantity tweened from the one
 * before it. Offsets keep their exact string form for display and gain a separate approximate number for geometry -
 * the two are never confused, because above 2^53 the number is wrong and the string is not.
 *
 * `partitions` IS DELIBERATELY LAZY. This runs on every animation frame, but the panel loop below is gated on
 * something having changed, and `renderStatus` reads none of it - so on a settled page (an idle healthy consumer,
 * which is most of the time) the whole per-partition tween would be computed sixty times a second and thrown away.
 * A getter rather than moving the call up into `frame` because the panels' contract stays exactly `view.partitions`:
 * nothing downstream has to know, and the work happens if and only if somebody actually reads it. Memoised, so a
 * panel reading it twice in one frame still pays once.
 */
function buildView(nowMonotonicMillis) {
    const current = feed.current;
    if (!current) {
        return null;
    }
    const previous = feed.previous;
    let fraction = 1;
    if (previous && !reducedMotion.matches) {
        // Sized from an ESTIMATE of the next gap, not from the last measured one. See tween.js - that asymmetry is
        // what stops the motion-pause-motion this page used to have.
        const durationMillis = feed.gapEstimateMillis || (current.monotonicMillis - previous.monotonicMillis);
        fraction = tweenFraction(nowMonotonicMillis - current.monotonicMillis, durationMillis);
    }
    const currentDocument = current.document;
    const previousDocument = previous ? previous.document : null;
    let interpolatedPartitions = null;

    return {
        fraction: fraction,
        settled: fraction >= 1,
        ageMillis: Math.max(0, Date.now() - current.wallClockMillis),
        receivedAtMillis: current.wallClockMillis,
        captureEpochMillis: currentDocument.captureEpochMillis,
        sampleSequence: currentDocument.sampleSequence,
        schemaVersion: currentDocument.schemaVersion,
        notice: currentDocument.notice,
        empty: currentDocument.empty === true,
        registryPopulated: currentDocument.registryPopulated === true,
        lifecycle: currentDocument.lifecycle || {},
        work: interpolateObject(previousDocument ? previousDocument.work : null, currentDocument.work, fraction),
        encoding: currentDocument.encoding || {},
        get partitions() {
            if (interpolatedPartitions === null) {
                interpolatedPartitions = interpolatePartitions(previousDocument, currentDocument, fraction);
            }
            return interpolatedPartitions;
        }
    };
}

function interpolateObject(previous, current, fraction) {
    const result = {};
    if (!current) {
        return result;
    }
    for (const key of Object.keys(current)) {
        const currentValue = current[key];
        if (typeof currentValue === 'number' || currentValue === null) {
            result[key] = interpolateNumber(previous ? previous[key] : null, currentValue, fraction);
        } else {
            result[key] = currentValue;
        }
    }
    return result;
}

function interpolatePartitions(previousDocument, currentDocument, fraction) {
    const previousByKey = new Map();
    if (previousDocument && Array.isArray(previousDocument.partitions)) {
        for (const partition of previousDocument.partitions) {
            previousByKey.set(partitionKey(partition), partition);
        }
    }
    const partitions = Array.isArray(currentDocument.partitions) ? currentDocument.partitions : [];
    return partitions.map(partition => {
        const key = partitionKey(partition);
        const before = previousByKey.get(key) || null;
        return {
            key: key,
            topic: partition.topic,
            partition: partition.partition,
            // exact, as strings, straight from the wire - this is what the reader is shown
            offsets: {
                committed: partition.lastCommittedOffset,
                sequential: partition.highestSequentialSucceededOffset,
                completed: partition.highestCompletedOffset,
                seen: partition.highestSeenOffset
            },
            // approximate, as numbers, tweened - this is what draws pixels and nothing else
            geometry: {
                committed: interpolateOffset(before, partition, 'lastCommittedOffset', fraction),
                sequential: interpolateOffset(before, partition, 'highestSequentialSucceededOffset', fraction),
                completed: interpolateOffset(before, partition, 'highestCompletedOffset', fraction),
                seen: interpolateOffset(before, partition, 'highestSeenOffset', fraction)
            },
            incompleteOffsets: interpolateNumber(before ? before.incompleteOffsets : null,
                partition.incompleteOffsets, fraction),
            processedRecords: interpolateNumber(before ? before.processedRecords : null,
                partition.processedRecords, fraction),
            failedRecords: interpolateNumber(before ? before.failedRecords : null,
                partition.failedRecords, fraction),
            slowRecords: interpolateNumber(before ? before.slowRecords : null, partition.slowRecords, fraction),
            assignmentEpoch: partition.assignmentEpoch,
            // carried by the document rather than recomputed here: the snapshot is the thing that knows whether the
            // four markers were read in a consistent instant
            offsetOrderingConsistent: partition.offsetOrderingConsistent !== false
        };
    });
}

/**
 * One offset marker's PIXEL number, tweened. The string-to-number step is `toGeometryNumber` rather than a `Number()`
 * cast written out again here: "the numeric form of an offset, for pixels only" is a rule with a documented reason
 * and a test, and it is worth exactly one definition.
 */
function interpolateOffset(before, current, field, fraction) {
    const to = toGeometryNumber(current[field]);
    if (to === null) {
        return null;
    }
    return interpolateNumber(toGeometryNumber(before ? before[field] : null), to, fraction);
}

function partitionKey(partition) {
    return partition.key || (partition.topic + '-' + partition.partition);
}

/* --------------------------------------------------------------- rendering */

const PHASES = {
    loading: {
        label: 'Connecting',
        detail: () => 'Asking the instance for its first sample.'
    },
    live: {
        label: 'Live',
        detail: view => transportWord() + ', sample ' + (view.sampleSequence || 0) + '.'
    },
    idle: {
        label: 'Idle',
        detail: () => 'Connected and up to date. Nothing in flight, nothing waiting, no incomplete offsets - this '
            + 'instance has nothing to do.'
    },
    stale: {
        label: 'Stale',
        detail: view => 'No new sample for ' + formatDuration(view.ageMillis) + '. The page is still connected, so '
            + 'the instance itself has stopped publishing - what is drawn below is the last good reading, ageing.'
    },
    nodata: {
        label: 'No Parallel Consumer meters',
        detail: () => 'The instance is answering, but the registry it was given carries no Parallel Consumer '
            + 'meters and no partitions. /status says which wiring step is missing.'
    },
    error: {
        label: 'Error',
        detail: () => feed.errorText || 'Something failed and did not say what.'
    },
    off: {
        label: 'Updates off',
        detail: () => 'This page is making no requests at all. Choose a cadence to resume.'
    }
};

function transportWord() {
    if (feed.transport === 'stream') {
        return 'streaming';
    }
    if (feed.transport === 'poll') {
        return 'polling every ' + formatDuration(pollIntervalMillis());
    }
    return feed.transport;
}

function currentPhase(view) {
    if (chosenCadence() === 'off') {
        return 'off';
    }
    if (!view) {
        return feed.errorText ? 'error' : 'loading';
    }
    if (feed.errorText) {
        return 'error';
    }
    if (view.ageMillis > staleThresholdMillis()) {
        return 'stale';
    }
    if (view.empty) {
        return 'nodata';
    }
    return isIdle(view) ? 'idle' : 'live';
}

/**
 * Idle is a deliberate, healthy state and has to be reachable: nothing in flight, nothing waiting, and nothing
 * incomplete. It is NOT inferred from a flat chart, because a flat chart is also what a stalled instance draws.
 */
function isIdle(view) {
    const work = view.work || {};
    return work.inflightRecords === 0 && work.waitingRecords === 0 && work.incompleteOffsetsTotal === 0;
}

function renderStatus(view) {
    const phase = currentPhase(view);
    const definition = PHASES[phase];
    setAttribute(dom.status, 'data-phase', phase);
    setText(dom.statusLabel, definition.label);
    const detail = definition.detail(view);
    setText(dom.statusDetail, detail);
    // the pill is a fixed width so a state change cannot re-wrap the toolbar, so the sentence is truncated - which
    // makes the tooltip the only way to read a long one, not a nicety
    setAttribute(dom.statusDetail, 'title', detail);

    if (view) {
        setText(dom.statusAge, formatDuration(view.ageMillis) + ' ago');
        setAttribute(dom.statusAge, 'title', 'Newest sample arrived at ' + formatInstant(view.receivedAtMillis)
            + '; the instance captured it at ' + formatInstant(view.captureEpochMillis) + '.');
    } else {
        setText(dom.statusAge, '');
    }

    const transportLabel = feed.transport === 'stream' ? 'stream'
        : (feed.transport === 'poll' ? 'polling' : feed.transport);
    setText(dom.transport, transportLabel);
    dom.transport.disabled = !feed.streamBlocked;
    setAttribute(dom.transport, 'title', feed.streamBlocked
        ? feed.streamBlockedReason + ' Click to try the event stream again.'
        : 'Updates arrive over server-sent events. This page falls back to polling on its own if the stream cannot '
        + 'be opened.');
}

function renderChrome(view) {
    if (!view) {
        return;
    }
    setText(dom.instanceId, view.lifecycle.instanceId || '');
    setText(dom.notice, view.notice || '');
    setText(dom.footerMeta, 'schema ' + view.schemaVersion + ' · sample ' + (view.sampleSequence || 0)
        + ' · captured ' + formatInstant(view.captureEpochMillis)
        + ' · every asset on this page is served by this instance; it makes no request to any other host.');
}

/**
 * The render loop, and the ONE place that must never stop.
 *
 * `requestAnimationFrame` schedules a single callback: a throw out of the frame body means the loop is never
 * re-armed, and the page freezes on whatever it last painted. That is the worst failure this page has, because it is
 * invisible - the numbers on screen are a real reading from a real instance, they simply stopped being current, and
 * every one of the explicit error and stale states below exists precisely so that cannot happen silently. So the
 * body is wrapped and the loop is re-armed unconditionally, in a `finally`.
 */
function frame(nowMonotonicMillis) {
    try {
        renderFrame(nowMonotonicMillis);
        clearFrameFailure();
    } catch (failure) {
        reportFrameFailure(failure);
    } finally {
        window.requestAnimationFrame(frame);
    }
}

function renderFrame(nowMonotonicMillis) {
    const view = buildView(nowMonotonicMillis);
    renderStatus(view);
    if (view && (dirty || !renderedSettled)) {
        renderChrome(view);
        for (const panel of panels) {
            panel.update(view);
        }
        dirty = false;
        renderedSettled = view.settled;
    }
}

/** The frame failure currently being shown, so it can be cleared when drawing starts working again. */
let frameFailureText = null;

/**
 * Surfaces a render failure through the page's existing error state rather than inventing a second one - `error` is
 * already a first-class phase with a label, a detail sentence and a tooltip.
 *
 * `dirty` is deliberately left alone: whatever set it stays set, so a transient failure retries on the next frame and
 * recovers on its own, while a permanent one is reported once to the console instead of sixty times a second.
 */
function reportFrameFailure(failure) {
    const text = 'The page failed while drawing - ' + describeFailure(failure) + '. What is on screen may be from an '
        + 'earlier sample even though the status above says otherwise. Reload the page; if it happens again, the '
        + 'browser console has the detail.';
    if (text !== frameFailureText) {
        // eslint-disable-next-line no-console
        console.error('Parallel Consumer dashboard: a render frame threw. The loop is still running.', failure);
    }
    frameFailureText = text;
    feed.errorText = text;
}

function clearFrameFailure() {
    if (frameFailureText === null) {
        return;
    }
    if (feed.errorText === frameFailureText) {
        // only OUR message is cleared - a transport error that arrived meanwhile is still true
        feed.errorText = null;
    }
    frameFailureText = null;
}

/* ------------------------------------------------------------------- start */

function mountPanels() {
    for (const panel of panels) {
        dom.panels.appendChild(panel.root);
    }
}

/**
 * One eager fetch before any transport starts. It buys two things at once: the first paint happens without waiting
 * for the stream's handshake, and the response header tells the page the server's own cadence.
 */
async function prime() {
    try {
        const response = await fetch(STATE_PATH, {headers: {Accept: 'application/json'}, cache: 'no-cache'});
        if (!response.ok) {
            throw new Error('the instance answered ' + response.status + ' ' + response.statusText);
        }
        readAdvertisedInterval(response);
        acceptDocument(await response.text(), 'the first read of the state document');
    } catch (failure) {
        feed.errorText = 'Could not read ' + STATE_PATH + ' - ' + describeFailure(failure) + '.';
        dirty = true;
    }
}

function start() {
    const remembered = readPersisted(CADENCE_STORAGE_KEY);
    if (remembered && Array.from(dom.cadence.options).some(option => option.value === remembered)) {
        dom.cadence.value = remembered;
    }
    dom.cadence.addEventListener('change', () => {
        writePersisted(CADENCE_STORAGE_KEY, chosenCadence());
        // an explicit choice is also a fresh chance for the stream, since "live" is the only setting it serves
        feed.streamBlocked = false;
        feed.errorText = null;
        consecutiveFailures = 0;
        applyTransport();
    });
    dom.transport.addEventListener('click', () => {
        feed.streamBlocked = false;
        feed.streamBlockedReason = null;
        feed.errorText = null;
        applyTransport();
    });
    document.addEventListener('visibilitychange', applyTransport);
    reducedMotion.addEventListener('change', () => {
        dirty = true;
    });

    mountPanels();
    window.requestAnimationFrame(frame);
    prime().then(applyTransport);
}

start();
