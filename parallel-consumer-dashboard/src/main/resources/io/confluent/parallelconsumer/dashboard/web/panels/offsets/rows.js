/*
 * Copyright (C) 2026 Antony Stubbs and contributors
 *
 * The per-partition row chrome that every offset style shares: the partition name, the won count, the ordering
 * caveat, the counterfactual sentence and the exact offset values. Only the graphic in the middle belongs to the
 * style, so only that is delegated.
 *
 * ROWS ARE REUSED, NEVER REBUILT. A row is created once per partition key and thereafter only has its text and
 * inline styles updated; re-sorting sets the flex `order` property rather than moving nodes. Rebuilding the list
 * every frame would flicker, would drop any text the reader had selected, and would throw away the renderer's own
 * state (a canvas, for a later style) sixty times a second.
 */

import {element, formatCount, formatOffset, setAttribute, setText} from '../../ui.js';

/**
 * @param container       where rows go
 * @param surfaceFactory  (surfaceElement) -> { update(model), destroy() }, the style's own graphic
 */
export function createRowList(container, surfaceFactory) {
    const rows = new Map();

    return {
        update(models) {
            const present = new Set();
            models.forEach((model, index) => {
                let row = rows.get(model.key);
                if (!row) {
                    row = createRow(surfaceFactory);
                    rows.set(model.key, row);
                    container.appendChild(row.root);
                }
                // sorting without touching the DOM tree - see the header
                if (row.root.style.order !== String(index)) {
                    row.root.style.order = String(index);
                }
                updateRow(row, model);
                row.surface.update(model);
                present.add(model.key);
            });
            for (const key of Array.from(rows.keys())) {
                if (!present.has(key)) {
                    const row = rows.get(key);
                    row.surface.destroy();
                    row.root.remove();
                    rows.delete(key);
                }
            }
        },

        destroy() {
            for (const row of rows.values()) {
                row.surface.destroy();
                row.root.remove();
            }
            rows.clear();
        }
    };
}

function createRow(surfaceFactory) {
    const root = element('div', 'pt');

    const head = element('div', 'pt-head');
    const name = element('span', 'pt-name');
    const won = element('span', 'pt-won');
    const caveat = element('span', 'badge badge-caveat', {
        hidden: 'hidden',
        title: 'The four offsets in this row were not read at one consistent instant, so their order cannot be '
            + 'relied on. The document says so itself; this page does not guess. The spans are drawn from what '
            + 'arrived and may not add up until the next sample.'
    });
    setText(caveat, 'markers sampled out of order');
    head.append(name, won, caveat);

    // its own line rather than trailing the head: it is a whole sentence, and squeezing it in beside the partition
    // name pushed it to the far right where it read as an afterthought
    const counterfactual = element('p', 'pt-note');

    const surfaceElement = element('div', 'pt-surface');

    const values = element('div', 'pt-values');
    const committed = createValue(values, 'Last committed',
        'The offset recorded with the broker. A restart resumes from here - and everything above it that is marked '
        + 'done in the commit metadata is not replayed.');
    const sequential = createValue(values, 'Highest sequential success',
        'The highest offset with an unbroken run of successes below it. This is what holds the base commit point '
        + 'back, and it is where a single-threaded consumer would be stopped.');
    const completed = createValue(values, 'Highest success',
        'The highest offset this instance has finished, out of order and ahead of the commit point.');
    const seen = createValue(values, 'Highest seen',
        'The highest offset polled from the broker, whether or not it has been worked yet.');
    const incomplete = createValue(values, 'Incomplete offsets',
        'How many offsets above the commit point are not done. These are the mechanism holding the base offset '
        + 'back, not damage - they are why the span above them had to be recorded in the commit metadata.');

    root.append(head, counterfactual, surfaceElement, values);

    return {
        root: root,
        name: name,
        won: won,
        caveat: caveat,
        counterfactual: counterfactual,
        committed: committed,
        sequential: sequential,
        completed: completed,
        seen: seen,
        incomplete: incomplete,
        surface: surfaceFactory(surfaceElement)
    };
}

function createValue(parent, label, title) {
    const wrapper = element('div', 'pt-value', {title: title});
    const labelNode = element('em');
    labelNode.style.fontStyle = 'normal';
    setText(labelNode, label);
    const valueNode = element('span');
    wrapper.append(labelNode, valueNode);
    parent.appendChild(wrapper);
    return valueNode;
}

function updateRow(row, model) {
    setText(row.name, model.label);

    const wonIsZero = model.wonCount === null || model.wonCount === 0n;
    setText(row.won, wonIsZero ? 'nothing won past the block' : '+' + model.wonCountText + ' won past the block');
    setAttribute(row.won, 'data-zero', wonIsZero ? 'true' : null);
    setAttribute(row.won, 'title', wonIsZero
        ? 'Nothing has been completed beyond the lowest incomplete offset on this partition yet.'
        : model.wonCountText + ' records finished beyond the point a single-threaded consumer would have stopped '
        + 'at. They are encoded into the commit metadata, so a restart does not replay them.');

    setAttribute(row.caveat, 'hidden', model.offsetOrderingConsistent ? 'hidden' : null);
    setText(row.counterfactual, counterfactualSentence(model));

    setText(row.committed, formatOffset(model.offsets.committed));
    setText(row.sequential, formatOffset(model.offsets.sequential));
    setText(row.completed, formatOffset(model.offsets.completed));
    setText(row.seen, formatOffset(model.offsets.seen));
    setText(row.incomplete, formatCount(model.incompleteOffsets));

    setAttribute(row.root, 'data-collapsed', model.collapsed ? 'true' : null);
}

/**
 * The sentence that states the point of the whole graphic. Note what it never says: the work above the commit point
 * is not pending, not uncommitted and not at risk. It is done, and it is recorded.
 */
function counterfactualSentence(model) {
    if (model.collapsed) {
        return 'Caught up - every offset seen has been processed and committed.';
    }
    if (!model.hasIncomplete) {
        return 'No incomplete offsets, so nothing is holding the commit point back.';
    }
    if (model.wonCount === null || model.wonCount === 0n) {
        return 'A single-threaded consumer would stop at offset ' + formatOffset(model.stopOffsetText)
            + ', and so far this instance has finished nothing beyond it.';
    }
    return 'A single-threaded consumer would stop at offset ' + formatOffset(model.stopOffsetText)
        + ' and reprocess everything above it. This instance has already finished ' + model.wonCountText
        + ' of those records, safely recorded in the commit metadata.';
}
