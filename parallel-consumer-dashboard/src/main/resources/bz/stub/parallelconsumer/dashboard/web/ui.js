/*
 * Copyright (C) 2026 Antony Stubbs and contributors
 *
 * Shared DOM and formatting helpers.
 *
 * These live in one module rather than being re-derived per panel: a second copy of "write this text only if it
 * changed" is a second place for a layout-shift bug to hide (plan R16).
 */

/**
 * Creates an element, optionally with a class, text and attributes. Attributes rather than properties, so `title`
 * and `data-*` read the same way at the call site.
 */
export function element(tag, className, attributes) {
    const node = document.createElement(tag);
    if (className) {
        node.className = className;
    }
    if (attributes) {
        for (const name of Object.keys(attributes)) {
            const value = attributes[name];
            if (value !== null && value !== undefined) {
                node.setAttribute(name, String(value));
            }
        }
    }
    return node;
}

/**
 * Writes text ONLY when it differs from what is already there.
 *
 * This is not a micro-optimisation. The render loop runs on every animation frame, and assigning `textContent`
 * unconditionally makes the browser re-lay-out the node sixty times a second whether or not the value moved -
 * which is exactly the flicker and reflow the page is required not to have.
 */
export function setText(node, value) {
    const text = value === null || value === undefined || value === '' ? ' ' : String(value);
    if (node.textContent !== text) {
        node.textContent = text;
    }
}

/**
 * Sets an attribute only when it differs. Same reasoning as {@link setText}, and it keeps CSS attribute selectors
 * from re-matching every frame.
 */
export function setAttribute(node, name, value) {
    const text = value === null || value === undefined ? null : String(value);
    if (text === null) {
        if (node.hasAttribute(name)) {
            node.removeAttribute(name);
        }
    } else if (node.getAttribute(name) !== text) {
        node.setAttribute(name, text);
    }
}

/**
 * Sets an inline style property only when it differs from what this function last wrote.
 *
 * The comparison is against a remembered value rather than against {@code style.getPropertyValue}, because the
 * browser normalises what it stores - a {@code calc()} expression comes back reformatted - so reading it back would
 * never match and every frame would write every property again.
 */
export function setStyle(node, property, value) {
    const remembered = '_lastStyle_' + property;
    if (node[remembered] === value) {
        return;
    }
    node[remembered] = value;
    node.style.setProperty(property, value);
}

export function clamp(value, low, high) {
    return value < low ? low : (value > high ? high : value);
}

/**
 * Linear interpolation that treats absence as absence (plan R38): a value that is null in either sample is not
 * tweened towards zero, because a missing reading and a measured zero are different facts.
 */
export function interpolateNumber(previous, current, fraction) {
    if (current === null || current === undefined) {
        return null;
    }
    if (previous === null || previous === undefined) {
        return Number(current);
    }
    const from = Number(previous);
    const to = Number(current);
    if (!Number.isFinite(from) || !Number.isFinite(to)) {
        return Number.isFinite(to) ? to : null;
    }
    return from + (to - from) * fraction;
}

/**
 * A count for display. Absent renders as a dash rather than as zero.
 */
export function formatCount(value) {
    if (value === null || value === undefined || !Number.isFinite(Number(value))) {
        return '-';
    }
    return Math.round(Number(value)).toLocaleString();
}

/**
 * A count held as a BigInt, so an offset difference wider than 2^53 still prints exactly.
 */
export function formatBigCount(value) {
    if (typeof value !== 'bigint') {
        return '-';
    }
    return value.toLocaleString();
}

/**
 * An exact offset for display. Printed as the string the server sent, character for character - never re-derived
 * from the number the geometry uses, which has already lost precision above 2^53.
 */
export function formatOffset(value) {
    if (value === null || value === undefined) {
        return '-';
    }
    return String(value);
}

/**
 * A short relative duration: "4s", "2m 05s", "1h 12m". Relative in the cell; the absolute instant goes on hover.
 */
export function formatDuration(millis) {
    if (millis === null || millis === undefined || !Number.isFinite(Number(millis))) {
        return '-';
    }
    const seconds = Math.max(0, Math.round(Number(millis) / 1000));
    if (seconds < 60) {
        return seconds + 's';
    }
    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) {
        return minutes + 'm ' + String(seconds % 60).padStart(2, '0') + 's';
    }
    const hours = Math.floor(minutes / 60);
    return hours + 'h ' + String(minutes % 60).padStart(2, '0') + 'm';
}

/**
 * An absolute wall-clock instant, for a tooltip.
 */
export function formatInstant(epochMillis) {
    if (epochMillis === null || epochMillis === undefined || !Number.isFinite(Number(epochMillis))) {
        return 'unknown';
    }
    return new Date(Number(epochMillis)).toLocaleTimeString();
}

/**
 * Parses an offset string into a BigInt for exact arithmetic, or null when absent or unparseable. Every count this
 * page derives from two offsets goes through here; none of them is computed from the numbers used for pixels.
 */
export function toBigInt(value) {
    if (value === null || value === undefined || value === '') {
        return null;
    }
    try {
        return BigInt(String(value));
    } catch (ignored) {
        return null;
    }
}

/**
 * The numeric form of an offset - for PIXELS ONLY. Above 2^53 this is approximate, which is fine for a bar that is
 * a few hundred pixels wide and never fine for anything the reader is shown as a value.
 */
export function toGeometryNumber(value) {
    if (value === null || value === undefined || value === '') {
        return null;
    }
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
}

/**
 * A remembered preference, or null if there is none or storage is unavailable.
 *
 * A browser with storage disabled - private mode, a locked-down enterprise profile, a sandboxed iframe - throws from
 * `localStorage` rather than returning nothing, and that is not a broken browser. It simply does not remember the
 * choice, which is a complete answer, so the throw is swallowed here once instead of at every call site.
 */
export function readPersisted(key) {
    try {
        return window.localStorage.getItem(key);
    } catch (ignored) {
        return null;
    }
}

/**
 * Remembers a preference, if this browser will let us. See {@link readPersisted} for why failing is not an error.
 */
export function writePersisted(key, value) {
    try {
        window.localStorage.setItem(key, value);
    } catch (ignored) {
        // nothing to do and nothing to report: the page works either way
    }
}
