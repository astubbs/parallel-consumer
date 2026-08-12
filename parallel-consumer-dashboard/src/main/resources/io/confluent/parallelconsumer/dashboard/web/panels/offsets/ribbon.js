/*
 * Copyright (C) 2026 Antony Stubbs and contributors
 *
 * The span ribbon: one horizontal bar per assigned topic-partition, on that partition's own offset axis, with the
 * four markers and the three spans between them.
 *
 * WHY PLAIN DOM AND NOT A CHART LIBRARY. This is a positional diagram, not a time series - there is no series to
 * plot and no axis to compute. Absolutely-positioned elements inside a track give exact percentage placement, cost
 * nothing to resize, never distort text the way a stretched viewBox does, and can carry a native tooltip per span.
 * A chart library here would be a dependency added to draw three rectangles.
 *
 * WHAT THE READER IS SUPPOSED TO TAKE AWAY. The green span is the point of the page: work already finished beyond
 * the offset a single-threaded consumer would have been stuck at, encoded into the commit metadata and therefore
 * safe across a restart. It is drawn as the loudest thing on the row. The dashed stop marker is the counterfactual
 * - it is achromatic on purpose, because it is not a state of the partition, it is where somebody else's consumer
 * would be. Alarm colours appear nowhere in this renderer: incomplete offsets are the mechanism that makes the won
 * span possible, not damage.
 */

import {element, formatOffset, setAttribute, setStyle} from '../../ui.js';
import {registerOffsetRenderer} from './registry.js';
import {createRowList} from './rows.js';

const RIBBON_LEGEND = [
    {
        kind: 'marker',
        name: 'Last committed',
        meaning: 'the offset the broker holds; a restart resumes here'
    },
    {
        kind: 'ready',
        name: 'Processed, ready to commit',
        meaning: 'an unbroken run of successes sitting above the commit point'
    },
    {
        kind: 'stop',
        name: 'A single-threaded consumer stops here',
        meaning: 'the lowest offset not yet done - one consumer thread could get no further'
    },
    {
        kind: 'won',
        name: 'Won: finished past that point',
        meaning: 'already processed and encoded into the commit metadata, so a restart does not replay it'
    },
    {
        kind: 'seen',
        name: 'Seen, not finished yet',
        meaning: 'polled from the broker and still to be worked'
    }
];

registerOffsetRenderer({
    id: 'ribbon',
    label: 'Span ribbon',
    description: 'One bar per partition on its own offset axis, with each span between the four markers drawn to '
        + 'scale. Best for seeing at a glance how far ahead of the commit point this instance is running.',
    legend: RIBBON_LEGEND,
    create(container) {
        const list = createRowList(container, createRibbonSurface);
        return {
            update(models) {
                list.update(models);
            },
            destroy() {
                list.destroy();
            }
        };
    }
});

function createRibbonSurface(surfaceElement) {
    const track = element('div', 'pt-track');

    const readySpan = element('div', 'pt-span', {'data-kind': 'ready'});
    const wonSpan = element('div', 'pt-span', {'data-kind': 'won'});
    const seenSpan = element('div', 'pt-span', {'data-kind': 'seen'});
    const committedMarker = element('div', 'pt-marker', {'data-kind': 'committed'});
    const stopMarker = element('div', 'pt-marker', {'data-kind': 'stop'});
    const seenMarker = element('div', 'pt-marker', {'data-kind': 'seen'});
    // spans first, markers second: markers must sit on top of the fills they delimit
    track.append(readySpan, wonSpan, seenSpan, committedMarker, stopMarker, seenMarker);

    const axis = element('div', 'pt-axis');
    const axisLow = element('span');
    const axisHigh = element('span');
    axis.append(axisLow, axisHigh);

    surfaceElement.append(track, axis);

    return {
        update(model) {
            const positions = model.positions;

            // The axis is per partition and its ends are printed, so a partition whose seen offset is millions
            // ahead of its commit cannot silently squash the informative spans of every other row.
            setAttribute(axis, 'title', 'This row has its own offset axis, so its width is not comparable with '
                + 'another row. The ends are the exact offsets below.');
            // the INCLUSIVE committed offset, because that is the position the track is actually drawn from - the
            // wire's committed value is one above it, and printing that would label the axis with a number the bar
            // does not start at
            axisLow.textContent = 'axis from ' + formatOffset(model.committedInclusiveText);
            axisHigh.textContent = 'to ' + formatOffset(model.offsets.seen);

            if (model.degenerateAxis) {
                // every marker on one offset: draw a single marker and let the healthy track styling carry the
                // meaning. Anything else would be dividing a zero-width axis into spans.
                hide(readySpan);
                hide(wonSpan);
                hide(seenSpan);
                hide(stopMarker);
                hide(seenMarker);
                show(committedMarker, 0.5);
                // the inclusive value again: this sentence CLAIMS the four markers are equal, and they are equal at
                // the committed offset's inclusive position, not at the exclusive one the wire carries
                setAttribute(committedMarker, 'title', 'Committed, succeeded and seen are all offset '
                    + formatOffset(model.committedInclusiveText) + ' - this partition is completely caught up. The '
                    + 'next offset to consume, which is what is committed to the broker, is '
                    + formatOffset(model.offsets.committed) + '.');
                return;
            }

            placeSpan(readySpan, positions.committed, positions.sequential, model.readyCount,
                'Processed and ready to commit: offsets ' + formatOffset(model.offsets.committed) + ' to '
                + formatOffset(model.offsets.sequential) + ' succeeded in an unbroken run.');
            placeSpan(wonSpan, positions.sequential, positions.completed, model.wonCount,
                model.wonCountText + ' records already finished beyond offset ' + formatOffset(model.stopOffsetText)
                + ', where a single-threaded consumer would have stopped. They are encoded into the commit '
                + 'metadata, so a restart does not replay them.');
            placeSpan(seenSpan, positions.completed, positions.seen, model.seenAheadCount,
                'Seen but not finished: offsets up to ' + formatOffset(model.offsets.seen)
                + ' have been polled and are still to be worked.');

            show(committedMarker, positions.committed);
            setAttribute(committedMarker, 'title', 'Last committed offset ' + formatOffset(model.offsets.committed)
                + '. A restart resumes here.');

            if (model.hasIncomplete) {
                show(stopMarker, positions.sequential);
                setAttribute(stopMarker, 'title', 'Offset ' + formatOffset(model.stopOffsetText)
                    + ' is the lowest one not yet done. A single-threaded consumer would be stopped here and would '
                    + 'have to reprocess everything above it.');
            } else {
                hide(stopMarker);
            }

            show(seenMarker, positions.seen);
            setAttribute(seenMarker, 'title', 'Highest seen offset ' + formatOffset(model.offsets.seen) + '.');
        },

        destroy() {
            surfaceElement.replaceChildren();
        }
    };
}

function placeSpan(node, from, to, count, title) {
    // Visibility is decided by the EXACT count, not by the pixel width: a span of one offset on an axis a million
    // wide is still a real span and gets its minimum width from the stylesheet.
    if (count === null || count === 0n) {
        hide(node);
        return;
    }
    setStyle(node, 'display', 'block');
    setStyle(node, 'left', axisOffset(from));
    setStyle(node, 'width', axisLength(Math.max(0, to - from)));
    setAttribute(node, 'title', title);
}

function show(node, position) {
    setStyle(node, 'display', 'block');
    setStyle(node, 'left', axisOffset(position));
}

function hide(node) {
    setStyle(node, 'display', 'none');
}

/**
 * A position along the track, with the axis inset from both ends.
 *
 * The inset is not decoration. A marker at either extreme - and the committed marker sits at zero on most healthy
 * partitions - would otherwise be drawn half outside the track and clipped, so the single most important glyph on
 * the graphic would be the one the reader could not see.
 */
function axisOffset(fraction) {
    return 'calc(var(--track-inset) + (100% - 2 * var(--track-inset)) * ' + fraction.toFixed(5) + ')';
}

function axisLength(fraction) {
    return 'calc((100% - 2 * var(--track-inset)) * ' + fraction.toFixed(5) + ')';
}
