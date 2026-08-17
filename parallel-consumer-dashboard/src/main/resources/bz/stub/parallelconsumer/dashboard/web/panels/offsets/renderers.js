/*
 * Copyright (C) 2026 Antony Stubbs and contributors
 *
 * The renderer manifest: the one and only file a new offset style edits.
 *
 * There is no build step and no bundler here, so nothing can enumerate a directory at load time - a module has to
 * be imported by name to exist. This file is that list, deliberately kept separate from the panel so that adding a
 * style never touches the panel, the page shell, or another style (KTD20, plan R49).
 *
 * Each module registers itself with the registry as a side effect of being imported.
 */

import './ribbon.js';
