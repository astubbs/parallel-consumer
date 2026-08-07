/*
 * Copyright (C) 2026 Antony Stubbs and contributors
 *
 * The renderer seam (KTD20).
 *
 * WHY A SEAM AT ALL, WITH ONE RENDERER IN IT. This graphic has no prior art anywhere - every Kafka tool that holds
 * this data prints it as adjacent numeric table cells - so there is no established form to copy and no way to know
 * from a drawing which reading works best under real load. The answer is to ship more than one and compare them in
 * use. Phase one ships exactly one so the visual language can be judged before it is multiplied.
 *
 * THE CONTRACT. A renderer is:
 *
 *   {
 *     id:          stable string, used as the persisted picker value
 *     label:       what the picker shows
 *     description: one sentence, shown under the picker
 *     legend:      [ { kind, name, meaning } ] - this style's own vocabulary; the panel draws it
 *     create(container) -> { update(models), destroy() }
 *   }
 *
 * `models` is the array from model.js, already sorted. A renderer owns everything inside its container and nothing
 * outside it. Adding a style is a new file plus one line in renderers.js - the panel, the page shell and the other
 * styles are not touched, which is the property R49 asks for and the reason the registry is not just an array
 * literal in the panel.
 */

const renderers = new Map();

export function registerOffsetRenderer(renderer) {
    if (!renderer || typeof renderer.id !== 'string' || renderer.id === '') {
        throw new Error('An offset renderer needs a non-empty string id.');
    }
    if (typeof renderer.create !== 'function') {
        throw new Error('Offset renderer "' + renderer.id + '" needs a create(container) function.');
    }
    if (renderers.has(renderer.id)) {
        throw new Error('An offset renderer with id "' + renderer.id + '" is already registered.');
    }
    renderers.set(renderer.id, renderer);
    return renderer;
}

/** Every registered style, in registration order. */
export function offsetRenderers() {
    return Array.from(renderers.values());
}

/** One style by id, or null. A persisted choice that no longer exists resolves to null and the panel falls back. */
export function offsetRenderer(id) {
    return renderers.get(id) || null;
}
