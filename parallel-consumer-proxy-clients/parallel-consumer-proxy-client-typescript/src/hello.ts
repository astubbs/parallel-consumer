// Copyright (C) 2026 Antony Stubbs and contributors

// Prints the one line bin/foreign-client-step.sh checks for. The TypeScript end of the polyglot
// build scaffolding (astubbs#242) - tsc really compiles this, so the module proves the compiler
// works rather than only that Node can execute a file.

const fixture: string = "parallel-consumer-proxy-client hello fixture: typescript";
process.stdout.write(fixture);
