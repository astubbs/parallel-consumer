# Copyright (C) 2026 Antony Stubbs and contributors
#
# The Python client's real build. Maven's pom.xml beside this file calls `make build` and
# `make test` through exec, under -Dpc.foreignClients only - so this is the one recipe, and a
# developer and the CI matrix row run exactly the same thing.

PYTHON ?= python3
VENV := .venv
BIN := $(VENV)/bin
REPO_ROOT := ../..
CLASSPATH_FILE := target/sidecar-classpath.txt

.PHONY: all build compile deps test lint ruff typecheck proto proto-check install clean distclean help

all: build test

help:
	@echo "build       deps + compile - what Maven's compile phase runs, and the whole of it"
	@echo "deps        install the package and its dev tools into $(VENV)"
	@echo "compile     parse every source file - the syntax check Maven's compile phase must fail on"
	@echo "test        run the suite (spawns the test-mode sidecar for the end-to-end test)"
	@echo "lint        ruff + mypy - the whole static analysis, and exactly what the CI row runs"
	@echo "ruff        the linter alone (pyflakes, bugbear, bandit)"
	@echo "typecheck   mypy alone: --strict over src, --check-untyped-defs over tests, tools and scripts"
	@echo "proto       regenerate the committed stubs from the frozen proxy.proto"
	@echo "proto-check regenerate and fail if the committed stubs have drifted"
	@echo "clean       remove the build tree, the caches and the cached sidecar classpath - NOT $(VENV)"
	@echo "distclean   clean, and remove $(VENV) too - a fresh 'make build' then refetches every wheel"

$(BIN)/python:
	$(PYTHON) -m venv $(VENV)

# WHAT MAVEN'S compile PHASE RUNS, and it is two steps because installing dependencies is not
# compiling. `build` used to be `deps` alone, and a module whose Python never parses is a module
# whose `mvn compile` reports SUCCESS on a source file containing `this is not valid python @@@` -
# measured, on this Makefile, before this target existed. Six of the eight foreign clients already
# went red on that sabotage; Python and Ruby did not, which is the check-that-passes-without-having-
# run class (docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md).
install build: deps compile

deps: $(BIN)/python
	$(BIN)/pip install --quiet --upgrade pip
	$(BIN)/pip install --quiet --editable '.[dev]'

# THE PARSE, AND ONLY THE PARSE. compileall walks each directory, byte-compiles every .py it finds,
# and exits non-zero printing CPython's own `SyntaxError` with the file, line and caret. That is the
# whole claim being made here: the sources are Python. It is the interpreter's own front end rather
# than a second implementation of it, which is why it is preferred to `ruff check` - ruff parses
# just as thoroughly and costs about the same, but it is the LINTER, and pointing the compile phase
# at it would make `mvn compile` go red on an unused import and make the lane sensitive to
# [tool.ruff] settings. `mypy --strict` was rejected on the same boundary plus cost: it catches
# strictly more, takes ~1s warm against 25ms here and far longer cold, and it is already `make lint`
# - the CI Python row runs that, so a type error is caught by the lane that owns it.
#
# WHAT IT DOES NOT CATCH, stated so the next reader does not over-trust it: anything past the
# grammar - undefined names, wrong types, imports that do not resolve - and any syntax that this
# interpreter accepts but the declared floor (requires-python = ">=3.10") does not, since it parses
# with whatever $(PYTHON) built $(VENV). Both belong to `make lint`.
#
# THE SAME FOUR DIRECTORIES AS `typecheck`, for the reason given there, and poc/ is excluded for the
# reason given in [tool.ruff] and [tool.mypy] in pyproject.toml: it is preserved evidence, not
# maintained source. Keep the three lists in step.
#
# Output lands in __pycache__ beside each source, which is already on both clean lists - the `clean`
# target below and the maven-clean-plugin filesets in pom.xml.
compile: deps
	$(BIN)/python -m compileall -q src tests tools scripts

test: build $(CLASSPATH_FILE)
	$(BIN)/python -m pytest -q

# STATIC ANALYSIS, and it is one target on purpose: the `clients` workflow's Python row runs
# `make lint` and nothing else, so a tool added here is a tool CI runs, with no second list to keep
# in step. Severity policy: everything below fails: there is no warn-only tier (astubbs#242,
# docs/client-static-analysis.md).
lint: ruff typecheck

ruff: build
	$(BIN)/ruff check .

# TWO PASSES, DIFFERENT STRICTNESS, and the split is the point - see the [tool.mypy] comment in
# pyproject.toml. src/ is the shipped surface and carries full annotations; tests get their bodies
# checked without every test function having to declare a signature. scripts/ joins them because the
# conformance runner lives there: it is the shared cross-language suite's only view of this client,
# so a runner that stopped compiling must redden this lane rather than the Java module's.
typecheck: build
	$(BIN)/mypy --strict src
	$(BIN)/mypy --check-untyped-defs tests tools scripts

proto: build
	$(BIN)/python tools/generate_proto.py

proto-check: build
	$(BIN)/python tools/generate_proto.py --check

# The classpath the end-to-end test needs to spawn the test-mode sidecar, written by this
# module's own Maven wiring (the python-e2e-harness profile in pom.xml).
#
# UNDER MAVEN THIS RULE NEVER RUNS: Maven writes the file at generate-test-resources, before it
# reaches the test phase that calls `make test`, so the file already exists and make skips the
# rule. The rule is for `make test` on its own, and it invokes the SAME wiring rather than a
# second recipe that could drift from it - stopping at test-compile, which is after the file is
# written and before the phase that would call back into make.
$(CLASSPATH_FILE):
	cd $(REPO_ROOT) && ./mvnw -q -pl :parallel-consumer-proxy-client-python -am \
		-Dpc.foreignClients -DskipTests test-compile

# WHAT `clean` MAY DELETE, AND WHY $(VENV) IS NOT ON THE LIST. Clean means: remove this module's
# build OUTPUT, leave its fetched dependencies alone. `mvn clean` deletes target/; it does not empty
# ~/.m2, and $(VENV) is this language's ~/.m2. Deleting it turns a sub-second clean into a
# multi-minute refetch of grpcio and mypy, which is `distclean` below and is a thing to ask for
# rather than a thing to get. Everything listed here was observed appearing from `make build`,
# `make test` and `make lint` on a tree with none of it present.
#
# NO TOOLCHAIN IS INVOKED. `rm` and `find` only, so this works on a checkout that has never built -
# where $(BIN)/python does not exist yet and any recipe reaching for it would exit non-zero.
#
# `mvn clean` REMOVES THE SAME PATHS BY A DIFFERENT ROUTE: pom.xml beside this file configures
# maven-clean-plugin with one fileset per path, because binding an exec of this target to the clean
# phase would make cleaning require a resolvable dependency tree. Two lists that must agree: change
# one, change the other.
clean:
	rm -rf target build dist src/*.egg-info .pytest_cache .ruff_cache .mypy_cache
	find src tests tools scripts -type d -name '__pycache__' -prune -exec rm -rf {} +

# The deeper clean, kept because the old `clean` did it and it is genuinely wanted when a
# dependency pin changes under an existing venv. Not what Maven runs.
distclean: clean
	rm -rf $(VENV)
