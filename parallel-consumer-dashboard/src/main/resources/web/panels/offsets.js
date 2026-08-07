/*
 * Copyright (C) 2026 Antony Stubbs and contributors
 *
 * The offsets panel: the page's headline graphic, and the frame around whichever style is drawing it.
 *
 * WHAT THIS FILE OWNS: the view model (delegated to model.js), the hero number, the legend, the style picker and
 * its persistence, and the empty states. WHAT IT DOES NOT OWN: how a partition is drawn. That is the registry's
 * business, and this file names no style - it asks the registry what exists. Adding a style therefore cannot
 * require an edit here (KTD20, plan R49).
 *
 * THE HERO NUMBER is highest-succeeded minus highest-sequential-succeeded, summed over the assigned partitions:
 * records this instance has processed that a consumer without this machinery could not have. It is computed in
 * BigInt from the string offsets, so it stays exact past 2^53, and it is deliberately the largest thing on the
 * page.
 */

import {element, formatBigCount, readPersisted, setAttribute, setText, writePersisted} from '../ui.js';
import {buildPartitionModels, totalWonRecords} from './offsets/model.js';
import {offsetRenderer, offsetRenderers} from './offsets/registry.js';
// side-effect import: this is what puts styles in the registry. See renderers.js for why it is a separate file.
import './offsets/renderers.js';

const STYLE_STORAGE_KEY = 'pc-dashboard.offsetStyle';

/** The picker value meaning "draw every registered style at once, so they can be compared on the same data". */
const COMPARE_ALL = 'all';

export function createOffsetsPanel() {
    const root = element('section', 'panel', {id: 'panel-offsets'});

    const head = element('div', 'panel-head');
    const heading = element('div');
    const title = element('h2', 'panel-title');
    setText(title, 'Running ahead of the commit point');
    const subtitle = element('p', 'panel-subtitle');
    const stylePicker = element('label', 'control', {
        title: 'How each partition is drawn. This graphic has no prior art anywhere, so more than one rendering '
            + 'ships and the reader chooses. Your choice is remembered in this browser.'
    });
    const styleLabel = document.createTextNode('Style ');
    const styleSelect = element('select', null, {id: 'offset-style'});
    stylePicker.append(styleLabel, styleSelect);
    heading.append(title, subtitle, stylePicker);

    const hero = element('div', 'hero', {
        title: 'Highest succeeded offset minus highest sequentially-succeeded offset, added up across every '
            + 'assigned partition. These records are finished and recorded in the commit metadata; a consumer '
            + 'without out-of-order processing would still have them ahead of it.'
    });
    const heroValue = element('span', 'hero-value');
    const heroLabel = element('span', 'hero-label');
    setText(heroLabel, 'records finished past the point a single-threaded consumer would have stopped at');
    hero.append(heroValue, heroLabel);

    head.append(heading, hero);

    const legend = element('div', 'legend');
    const stage = element('div');
    const empty = element('p', 'pt-empty', {hidden: 'hidden'});

    root.append(head, legend, stage, empty);

    /** { id, renderer, container, instance } for each style currently mounted. */
    let mounted = [];
    let mountedChoice = null;

    function availableChoices() {
        const choices = offsetRenderers().map(renderer => ({value: renderer.id, label: renderer.label}));
        if (choices.length > 1) {
            choices.push({value: COMPARE_ALL, label: 'All styles (compare)'});
        }
        return choices;
    }

    function populatePicker() {
        const choices = availableChoices();
        styleSelect.replaceChildren();
        for (const choice of choices) {
            const option = element('option', null, {value: choice.value});
            setText(option, choice.label);
            styleSelect.appendChild(option);
        }
        const remembered = readPersisted(STYLE_STORAGE_KEY);
        const valid = choices.some(choice => choice.value === remembered);
        styleSelect.value = valid ? remembered : (choices.length > 0 ? choices[0].value : '');
        // one style is not a choice; say so rather than offering a control that does nothing
        styleSelect.disabled = choices.length < 2;
        setAttribute(stylePicker, 'title', choices.length < 2
            ? 'One rendering of this data ships today. The picker is here because further styles plug in without '
            + 'touching this panel, and it will offer them when they land.'
            : 'How each partition is drawn. Your choice is remembered in this browser.');
    }

    function mountChoice() {
        const choice = styleSelect.value;
        if (choice === mountedChoice) {
            return;
        }
        for (const entry of mounted) {
            entry.instance.destroy();
            entry.group.remove();
        }
        mounted = [];

        const chosen = choice === COMPARE_ALL ? offsetRenderers() : [offsetRenderer(choice)].filter(Boolean);
        const renderers = chosen.length > 0 ? chosen : offsetRenderers();

        for (const renderer of renderers) {
            // one group per style, so unmounting a style takes its caption with it and comparison mode cannot
            // leave orphaned headings behind
            const group = element('div', 'style-group');
            if (renderers.length > 1) {
                const caption = element('p', 'panel-subtitle');
                setText(caption, renderer.label + ' - ' + renderer.description);
                group.appendChild(caption);
            }
            const container = element('div', 'ribbons');
            group.appendChild(container);
            stage.appendChild(group);
            mounted.push({
                id: renderer.id,
                renderer: renderer,
                group: group,
                container: container,
                instance: renderer.create(container)
            });
        }

        setText(subtitle, renderers.length === 1 ? renderers[0].description
            : 'Every registered style, drawn against the same data.');
        renderLegend(legend, renderers);
        mountedChoice = choice;
    }

    populatePicker();
    mountChoice();

    styleSelect.addEventListener('change', () => {
        writePersisted(STYLE_STORAGE_KEY, styleSelect.value);
        mountChoice();
    });

    return {
        root: root,

        update(view) {
            const models = buildPartitionModels(view);

            const won = totalWonRecords(models);
            setText(heroValue, won === 0n ? '0' : '+' + formatBigCount(won));
            setAttribute(hero, 'data-zero', won === 0n ? 'true' : null);

            if (models.length === 0) {
                setAttribute(empty, 'hidden', null);
                setText(empty, view.empty
                    ? 'No Parallel Consumer meters were found in the registry this dashboard was given, so there '
                    + 'is nothing to draw. /status names the wiring step that is missing.'
                    : 'No partitions are assigned to this instance right now. That is what a consumer between '
                    + 'rebalances looks like, and it is not an error.');
            } else {
                setAttribute(empty, 'hidden', 'hidden');
            }

            for (const entry of mounted) {
                entry.instance.update(models);
            }
        }
    };
}

/**
 * Draws the active style's own vocabulary. Every span and marker is named on first view: a survey of the existing
 * Kafka tooling found no prior art for this graphic anywhere, so no reader arrives with an intuition for it and the
 * picture emphatically does not speak for itself.
 */
function renderLegend(container, renderers) {
    container.replaceChildren();
    const seen = new Set();
    for (const renderer of renderers) {
        for (const item of renderer.legend || []) {
            const identity = item.kind + '|' + item.name;
            if (seen.has(identity)) {
                continue;
            }
            seen.add(identity);
            const entry = element('span', 'legend-item', {title: item.meaning});
            const swatch = element('span', 'legend-swatch', {'data-kind': item.kind});
            const text = element('span');
            const strong = element('b');
            setText(strong, item.name);
            text.append(strong, document.createTextNode(' - ' + item.meaning));
            entry.append(swatch, text);
            container.appendChild(entry);
        }
    }
}
