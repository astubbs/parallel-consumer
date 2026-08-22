// Copyright (C) 2026 Antony Stubbs and contributors

/**
 * The module's bug-finding lint, not a style checker.
 *
 * The rules that earn their place here are the TYPE-AWARE ones - `recommendedTypeChecked` plus the
 * three additions below - because untyped ESLint on TypeScript finds formatting and little else.
 * The ones this codebase is shaped to need are the floating-promise family: a client library whose
 * transport, executors and shutdown are all async has exactly one silent failure mode, which is an
 * unawaited promise whose rejection nobody sees. `no-floating-promises` and `no-misused-promises`
 * are the checks that see it, and they cannot run without the type information the tsconfig
 * project supplies.
 *
 * ONE COMMAND, TWO PLACES: `npm run lint` runs `eslint .`, which is exactly what the CI matrix row
 * runs (`npx --no-install eslint .`). `npm run check` is the local gate - `tsc --build` for the
 * compiler's half, then this - so a developer runs the same two checks CI does, in one command,
 * before pushing.
 */

import js from "@eslint/js";
import tseslint from "typescript-eslint";

export default tseslint.config(
  {
    // Build output, dependencies, and protoc's own output. The generated stubs are the generator's
    // code, not this project's: linting them would either fail on style this project does not own
    // or be undone by the next `npm run proto`. They ARE type-checked - see tsconfig.json.
    ignores: ["dist/**", "node_modules/**", "src/generated/**"],
  },
  js.configs.recommended,
  ...tseslint.configs.recommendedTypeChecked,
  {
    languageOptions: {
      parserOptions: {
        project: ["./tsconfig.json"],
        tsconfigRootDir: import.meta.dirname,
      },
    },
    rules: {
      // The three that matter for this module's shape, raised from their defaults.
      "@typescript-eslint/no-floating-promises": "error",
      "@typescript-eslint/no-misused-promises": "error",
      "@typescript-eslint/require-await": "error",
    },
  },
  {
    // Build scripts are plain ESM JavaScript and sit outside the tsconfig project, so they get the
    // untyped rules only. Nothing in them is on the library's hot path.
    files: ["**/*.mjs"],
    ...tseslint.configs.disableTypeChecked,
    languageOptions: {
      ...tseslint.configs.disableTypeChecked.languageOptions,
      // The two Node globals the scripts use, named rather than pulled in as a whole environment
      // through another dependency.
      globals: { console: "readonly", process: "readonly" },
    },
  },
);
