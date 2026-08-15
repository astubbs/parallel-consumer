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

.PHONY: all build test lint ruff typecheck proto proto-check install clean help

all: build test

help:
	@echo "build       install the package and its dev tools into $(VENV)"
	@echo "test        run the suite (spawns the test-mode sidecar for the end-to-end test)"
	@echo "lint        ruff + mypy - the whole static analysis, and exactly what the CI row runs"
	@echo "ruff        the linter alone (pyflakes, bugbear, bandit)"
	@echo "typecheck   mypy alone: --strict over src, --check-untyped-defs over tests, tools and scripts"
	@echo "proto       regenerate the committed stubs from the frozen proxy.proto"
	@echo "proto-check regenerate and fail if the committed stubs have drifted"
	@echo "clean       remove the venv, the build tree and the cached sidecar classpath"

$(BIN)/python:
	$(PYTHON) -m venv $(VENV)

install build: $(BIN)/python
	$(BIN)/pip install --quiet --upgrade pip
	$(BIN)/pip install --quiet --editable '.[dev]'

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

clean:
	rm -rf $(VENV) target build dist src/*.egg-info .pytest_cache
	find src tests tools -name '__pycache__' -type d -exec rm -rf {} +
