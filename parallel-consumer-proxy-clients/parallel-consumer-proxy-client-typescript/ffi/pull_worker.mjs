// Copyright (C) 2026 Antony Stubbs and contributors

// Blocks in pc_next on a worker thread, so the main thread's event loop can be observed while it
// happens. The worker attaches its OWN isolate thread - a GraalVM isolate thread belongs to the OS
// thread it was attached on, so the main thread's cannot be reused here.

import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parentPort, workerData } from 'node:worker_threads';

const here = path.dirname(fileURLToPath(import.meta.url));
const pc = createRequire(import.meta.url)(path.join(here, 'pc_addon.node'));

const thread = pc.attachThread(workerData.isolate);
const buffer = Buffer.alloc(64 * 1024);
const started = Date.now();
const result = pc.next(thread, workerData.handle, buffer, workerData.timeoutMillis);
parentPort.postMessage({ rc: result.rc, elapsed: Date.now() - started });
