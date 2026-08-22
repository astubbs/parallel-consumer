/* Copyright (C) 2026 Antony Stubbs and contributors
 *
 * The two animations on the landing page. Hand-written, no dependencies, no network.
 *
 * Both are driven from markup that is already in a meaningful state before this file runs:
 * the offset map is authored in its post-recovery frame and the shard view mid-run, so a reader
 * with JavaScript off, or with `prefers-reduced-motion: reduce`, sees the punchline of each
 * diagram rather than an empty box. This file only animates the way each of those frames is
 * arrived at.
 */
(function () {
    'use strict';

    var reduced = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;

    /* ---------------------------------------------------------------------
       Small helpers
       --------------------------------------------------------------------- */

    function clamp01(v) { return v < 0 ? 0 : (v > 1 ? 1 : v); }

    function group(n) { return String(n).replace(/\B(?=(\d{3})+(?!\d))/g, ','); }

    /* A fixed permutation, so "out of order" looks the same on every replay and every machine. */
    function shuffled(list, seed) {
        var a = list.slice(), s = seed;
        for (var i = a.length - 1; i > 0; i--) {
            s = (s * 1103515245 + 12345) % 2147483648;
            var j = s % (i + 1);
            var t = a[i]; a[i] = a[j]; a[j] = t;
        }
        return a;
    }

    /* Run an animation only while it is on screen. */
    function whenVisible(el, onEnter, onLeave) {
        if (!('IntersectionObserver' in window)) { onEnter(); return; }
        new IntersectionObserver(function (entries) {
            entries.forEach(function (e) { e.isIntersecting ? onEnter() : onLeave(); });
        }, { threshold: 0.25 }).observe(el);
    }

    /* ---------------------------------------------------------------------
       1. THE OFFSET MAP
       A partition as a row of offsets. They complete out of order; one sticks and three stay
       in flight; the commit is taken at the lowest incomplete offset and carries the completion
       set that follows it; the process dies; only the four outstanding records come back.
       --------------------------------------------------------------------- */

    function offsetMap() {
        var root = document.getElementById('offsetViz');
        if (!root) { return; }

        var row = root.querySelector('#offsetRow');
        var cells = Array.prototype.slice.call(row.querySelectorAll('rect.cell'));
        var marker = root.querySelector('#commitMarker');
        var bracket = root.querySelector('#metaBracket');
        var crash = root.querySelector('#crashLine');
        var caption = root.querySelector('#omCaption');
        var state = root.querySelector('#omState');
        var dots = Array.prototype.slice.call(root.querySelectorAll('#omPhases span'));
        var replay = root.querySelector('#omReplay');

        var PITCH = 15;
        var STUCK = 21;
        var FLIGHT = [20, 22, 23];
        var OUTSTANDING = [20, 21, 22, 23];
        var COMMIT_AT = 20;                       /* the lowest offset not yet complete */
        var LOOP = 12600;

        var completable = [];
        for (var i = 0; i < cells.length; i++) {
            if (OUTSTANDING.indexOf(i) === -1) { completable.push(i); }
        }
        var order = shuffled(completable, 20260821);

        var timers = [];
        var running = false;
        var doneCount = 0;

        function at(t, fn) { timers.push(setTimeout(fn, t)); }
        function clear() { timers.forEach(clearTimeout); timers = []; }

        function set(i, cls) { cells[i].setAttribute('class', 'cell' + (cls ? ' ' + cls : '')); }

        function phase(n, html, note) {
            dots.forEach(function (d, k) { d.className = k === n ? 'is-on' : ''; });
            caption.innerHTML = html;
            state.textContent = note;
        }

        function moveMarker(index, animate) {
            if (!animate) { marker.style.transition = 'none'; }
            marker.style.transform = 'translateX(' + (index * PITCH) + 'px)';
            if (!animate) {
                void marker.getBoundingClientRect();
                marker.style.transition = '';
            }
        }

        function reset() {
            clear();
            doneCount = 0;
            row.classList.remove('is-dimmed');
            crash.classList.remove('is-on');
            bracket.classList.remove('is-on');
            cells.forEach(function (c, i) { set(i, ''); });
            moveMarker(0, false);
        }

        function play() {
            reset();

            /* -- 1. arrival ------------------------------------------------ */
            phase(0, '<b>Arrival.</b> 44 records land in one partition. None of them is complete yet.',
                '0 of 44 complete');

            /* -- 2. concurrent processing, completing out of order --------- */
            at(800, function () {
                phase(1, '<b>Processing.</b> Work runs concurrently, so records complete out of ' +
                    'order. One record sticks; three more are still in flight.',
                    '0 of 44 complete');
            });
            order.forEach(function (idx, k) {
                at(900 + k * 58, function () {
                    set(idx, 'is-done');
                    doneCount++;
                    state.textContent = doneCount + ' of 44 complete';
                });
            });
            at(1500, function () { set(STUCK, 'is-stuck'); });
            at(1900, function () { FLIGHT.forEach(function (f) { set(f, 'is-flight'); }); });

            /* -- 3. the commit, carrying the completion set ---------------- */
            at(3500, function () {
                phase(2, '<b>The commit.</b> It is taken at the lowest offset that is not yet ' +
                    'complete, and it carries the set of completions that follow it.',
                    'commit at the lowest incomplete offset · 4 outstanding');
                moveMarker(COMMIT_AT, true);
            });
            at(4200, function () { bracket.classList.add('is-on'); });

            /* -- 4. the crash ---------------------------------------------- */
            at(5600, function () {
                phase(3, '<b>The crash.</b> The process dies. Everything still in flight is lost.',
                    'process died · partition reassigned');
                crash.classList.add('is-on');
            });
            at(6400, function () { row.classList.add('is-dimmed'); });

            /* -- 5. recovery ----------------------------------------------- */
            at(7200, function () {
                row.classList.remove('is-dimmed');
                crash.classList.remove('is-on');
                cells.forEach(function (c, i) {
                    if (OUTSTANDING.indexOf(i) === -1) { set(i, 'is-kept'); }
                    else { set(i, ''); }
                });
                phase(4, '<b>Recovery.</b> Only the four records that never completed are ' +
                    'processed again. The 40 recorded as done stay done.',
                    '4 records processed again · 40 not');
            });
            OUTSTANDING.forEach(function (idx, k) {
                at(7600 + k * 260, function () { set(idx, 'is-replay'); });
            });

            at(LOOP, function () { if (running) { play(); } });
        }

        /* Reduced motion: the markup is already authored in the recovery frame. Leave it, and make
           sure the phase indicator agrees with what is on screen. */
        if (reduced) {
            dots.forEach(function (d, k) { d.className = k === 4 ? 'is-on' : ''; });
            replay.disabled = true;
            replay.textContent = 'Static';
            replay.title = 'Animation is disabled because your system asks for reduced motion. ' +
                'The final frame is shown instead.';
            return;
        }

        replay.addEventListener('click', function () { running = true; play(); });

        whenVisible(root,
            function () { if (!running) { running = true; play(); } },
            function () { running = false; clear(); });
    }

    /* ---------------------------------------------------------------------
       2. SHARD DISPATCH
       1 record on key A, 249 on key B, and 1,000 records each with a distinct key. Every record
       is placed into its shard on arrival; dispatch then runs across every shard at once, so the
       1,001 independent records finish while B drains in order.
       --------------------------------------------------------------------- */

    function shardDispatch() {
        var root = document.getElementById('shardViz');
        if (!root) { return; }

        var drainB = root.querySelector('#shardBDrain');
        var sweep = root.querySelector('#fieldSweep');
        var edge = root.querySelector('#sweepEdge');
        var dotA = root.querySelector('#shardADot');
        var cntA = root.querySelector('#cntA');
        var cntB = root.querySelector('#cntB');
        var cntF = root.querySelector('#cntField');
        var call1 = root.querySelector('#callout1');
        var call2 = root.querySelector('#callout2');
        var caption = root.querySelector('#sdCaption');
        var replay = root.querySelector('#sdReplay');

        var COL = 9.5, ROW = 2.2892;              /* the dot pitch of each field */
        var FIELD_W = 380, FIELD_H = 190;
        var DURATION = 8200, HOLD = 1600;

        var raf = null, started = 0, running = false;

        function frame(p) {
            /* key A: one record, done as soon as dispatch starts */
            var aDone = p >= 0.05;
            dotA.setAttribute('fill', aDone ? 'var(--c-accent)' : 'var(--c-rule-firm)');
            cntA.textContent = (aDone ? '1' : '0') + ' / 1';

            /* the 1,000 single-record shards: swept in column order, 25 records per column */
            var cols = Math.round(clamp01((p - 0.05) / 0.17) * (FIELD_W / COL));
            sweep.setAttribute('width', (cols * COL).toFixed(2));
            cntF.textContent = group(cols * 25) + ' / 1,000';
            edge.setAttribute('opacity', (cols > 0 && cols < 40) ? '1' : '0');
            edge.setAttribute('x1', (cols * COL).toFixed(2));
            edge.setAttribute('x2', (cols * COL).toFixed(2));

            /* key B: 249 records, 3 per row, draining in order at its own pace */
            var rows = Math.round(clamp01((p - 0.03) / 0.92) * (FIELD_H / ROW));
            drainB.setAttribute('height', (rows * ROW).toFixed(2));
            cntB.textContent = Math.min(rows * 3, 249) + ' / 249';

            call1.classList.toggle('is-on', p >= 0.24);
            call2.classList.toggle('is-on', p >= 0.985);

            var text;
            if (p < 0.05) {
                text = '<b>Arrival.</b> All 1,251 records are placed into their shards. There is no ' +
                    'admission stage for them to queue at.';
            } else if (p < 0.24) {
                text = '<b>Dispatch.</b> The control loop dispatches across every shard at once, ' +
                    'up to the in-flight target.';
            } else if (p < 0.985) {
                text = '<b>Mid-run.</b> The 1,000 single-record shards and key <code>A</code> are ' +
                    'finished. Key <code>B</code> proceeds in order on its own shard, and nothing ' +
                    'else is waiting on it.';
            } else {
                text = '<b>Done.</b> <code>B</code>’s 249 records took as long as they take. ' +
                    'The other 1,001 did not wait for them.';
            }
            if (caption.innerHTML !== text) { caption.innerHTML = text; }
        }

        function tick(now) {
            if (!running) { return; }
            var elapsed = now - started;
            if (elapsed > DURATION + HOLD) { started = now; elapsed = 0; }
            frame(clamp01(elapsed / DURATION));
            raf = requestAnimationFrame(tick);
        }

        function start() {
            if (running) { return; }
            running = true;
            started = performance.now();
            raf = requestAnimationFrame(tick);
        }

        function stop() {
            running = false;
            if (raf) { cancelAnimationFrame(raf); raf = null; }
        }

        /* Reduced motion: the markup is already authored at the mid-run frame - the 1,001
           independent records finished, B still draining. That is the whole point of the diagram. */
        if (reduced) {
            replay.disabled = true;
            replay.textContent = 'Static';
            replay.title = 'Animation is disabled because your system asks for reduced motion. ' +
                'The mid-run frame is shown instead.';
            return;
        }

        replay.addEventListener('click', function () { stop(); start(); });
        whenVisible(root, start, stop);
    }

    offsetMap();
    shardDispatch();
})();
