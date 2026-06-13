SHELL := /bin/bash
.SHELLFLAGS := -o pipefail -c
PYTHON ?= python
NPM ?= npm
NODE ?= node
TWINE ?= twine
PYTEST ?= $(PYTHON) -m pytest

.PHONY: all build test clean wasm app site verify fuzz
.PHONY: build-c build-debug build-node build-wasm build-js build-py sync-js-csrc sync-py-csrc
.PHONY: check-js-csrc-sync check-py-csrc-sync check-csrc-sync test-packaging test-packaging-install
.PHONY: test-packaging-node test-packaging-node-install test-packaging-python test-packaging-python-install test-properties
.PHONY: test-c test-memory test-debug test-python test-node test-parity
.PHONY: publish-python publish-node publish-github

all: build test

# --- Build targets ---

build: build-c build-node

build-c:
	@mkdir -p build
	@cd build && cmake .. -DBUILD_TESTING=ON -DCMAKE_BUILD_TYPE=Release > /dev/null 2>&1
	@cd build && make -j$$(nproc) 2>&1 | tail -1
	@echo "  C core OK"

build-debug:
	@mkdir -p build-debug
	@cd build-debug && cmake .. -DBUILD_TESTING=ON -DCMAKE_BUILD_TYPE=Debug > /dev/null 2>&1
	@cd build-debug && make -j$$(nproc) 2>&1 | tail -1
	@echo "  C core (Debug+ASan/UBSan) OK"

sync-js-csrc:
	@cd js && $(NODE) scripts/sync-csrc.js

sync-py-csrc:
	@cd py && $(PYTHON) scripts/sync-csrc.py

check-js-csrc-sync:
	@$(PYTHON) scripts/check-csrc-sync.py --mirror js

check-py-csrc-sync:
	@$(PYTHON) scripts/check-csrc-sync.py --mirror py

check-csrc-sync:
	@$(PYTHON) scripts/check-csrc-sync.py

build-node: build-c sync-js-csrc check-js-csrc-sync
	@cd js && $(NPM) run build:native 2>&1 | tail -1
	@echo "  Node.js N-API OK"

build-js: build-node build-wasm

build-py: sync-py-csrc check-py-csrc-sync
	@cd py && rm -rf dist build *.egg-info && $(PYTHON) -m build --sdist
	@$(PYTHON) scripts/audit-package-artifacts.py --python-sdist "py/dist/tranfi-*.tar.gz"

build-wasm:
	@bash scripts/build-wasm.sh 2>&1 | tail -3

wasm: build-wasm

# --- Test targets ---

test: test-c test-python test-node test-packaging

test-c: build-c
	@cmake --build build --target test_memory test_core > /dev/null
	@./build/test_memory
	@./build/test_core
	@bash test/test_cli_memory_policy.sh ./build/tranfi

test-memory: build-c
	@cmake --build build --target test_memory > /dev/null
	@./build/test_memory

test-debug: build-debug
	@ASAN_OPTIONS=detect_leaks=0 ./build-debug/test_core
	@ASAN_OPTIONS=detect_leaks=0 ./build-debug/test_memory
	@ASAN_OPTIONS=detect_leaks=0 bash test/test_cli_memory_policy.sh ./build-debug/tranfi

test-python: build-c
	@PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 TRANFI_LIB_PATH=build/libtranfi.so \
		$(PYTEST) test/test_python.py test/test_parity.py -v --tb=short

test-node: build-node
	@$(NODE) test/test_node.js

test-parity: build-c
	@PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 TRANFI_LIB_PATH=build/libtranfi.so \
		$(PYTEST) test/test_parity.py -v --tb=short

test-properties: build-c
	@PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 TRANFI_LIB_PATH=build/libtranfi.so \
		$(PYTEST) test/test_properties.py -v --tb=short

test-packaging: check-csrc-sync test-packaging-python test-packaging-node test-packaging-install

test-packaging-python: sync-py-csrc check-py-csrc-sync
	@cd py && rm -rf dist build *.egg-info && $(PYTHON) -m build --sdist
	@$(PYTHON) scripts/audit-package-artifacts.py --python-sdist "py/dist/tranfi-*.tar.gz"

test-packaging-node: sync-js-csrc check-js-csrc-sync
	@mkdir -p build
	@cd js && npm_config_cache=/tmp/npm-pack-audit $(NPM) pack --dry-run --json > ../build/npm-pack-dry-run.json
	@$(PYTHON) scripts/audit-package-artifacts.py --npm-json build/npm-pack-dry-run.json

test-packaging-python-install: test-packaging-python
	@$(PYTHON) scripts/smoke-install-packages.py --python-sdist "py/dist/tranfi-*.tar.gz" --python "$(PYTHON)"

test-packaging-node-install: test-packaging-node
	@rm -rf build/npm-install-smoke && mkdir -p build/npm-install-smoke
	@cd js && npm_config_cache=/tmp/npm-pack-audit $(NPM) pack --json --pack-destination ../build/npm-install-smoke > ../build/npm-pack-install.json
	@$(PYTHON) scripts/audit-package-artifacts.py --npm-json build/npm-pack-install.json
	@$(PYTHON) scripts/smoke-install-packages.py --npm-tarball "build/npm-install-smoke/tranfi-*.tgz" --node "$(NODE)" --npm "$(NPM)"

test-packaging-install: test-packaging-python-install test-packaging-node-install

# --- Fuzz testing ---

fuzz: build/fuzz_csv
	@mkdir -p corpus/csv
	./build/fuzz_csv corpus/csv -max_len=4096 -timeout=5

FUZZ_SRC = $(filter-out src/main.c,$(wildcard src/*.c))
build/fuzz_csv:
	@mkdir -p build
	clang -std=c11 -g -O1 -fsanitize=fuzzer,address,undefined \
		-D_POSIX_C_SOURCE=200809L -I src \
		test/fuzz_csv.c $(FUZZ_SRC) -lm -o build/fuzz_csv

# --- Verify (full suite with sanitizers) ---

verify: build-debug test-debug test-python test-node test-packaging

# --- App targets ---

app: build-wasm
	@mkdir -p app/public/wasm
	@cp js/wasm/tranfi_core.js app/public/wasm/tranfi_core.js
	@echo "  WASM copied to app/public/wasm/"

site: app
	@cd app && npx vite build 2>&1 | tail -1
	@node scripts/build-site.js --out _site --base /
	@echo "  Site OK → _site/"

# --- Publish targets ---

publish-python:
	@cd py && rm -rf dist && \
		$(PYTHON) scripts/sync-csrc.py && \
		$(PYTHON) ../scripts/check-csrc-sync.py --mirror py && \
		rm -rf tranfi/app && cp -r ../app/dist tranfi/app && rm -rf tranfi/app/wasm tranfi/app/lib && \
		$(PYTHON) -m build --sdist && \
		$(PYTHON) ../scripts/audit-package-artifacts.py --python-sdist "dist/tranfi-*.tar.gz" && \
		$(PYTHON) ../scripts/smoke-install-packages.py --python-sdist "dist/tranfi-*.tar.gz" --python "$(PYTHON)" && \
		$(TWINE) upload dist/*.tar.gz

publish-node: build-js test-packaging-node-install
	@cd js && $(NPM) publish

publish-github:
	@bash scripts/release.sh

# --- Coverage ---

COV_SRC = $(filter-out src/main.c,$(wildcard src/*.c))
coverage: build/test_core_cov
	@./build/test_core_cov
	@gcov -o build/cov $(COV_SRC) > /dev/null 2>&1
	@echo "  Coverage files: *.gcov"
	@echo "  Summary:"
	@for f in src/*.c; do \
		pct=$$(gcov -n -o build/cov "$$f" 2>/dev/null | grep -oP '\d+\.\d+%' | head -1); \
		[ -n "$$pct" ] && printf "    %-30s %s\n" "$$(basename $$f)" "$$pct"; \
	done

build/test_core_cov: $(COV_SRC) test/test_core.c
	@mkdir -p build/cov
	@cd build && cmake .. -DBUILD_TESTING=ON -DCMAKE_BUILD_TYPE=Debug \
		-DCMAKE_C_FLAGS="-fprofile-arcs -ftest-coverage -UNDEBUG" > /dev/null 2>&1
	@cd build && make -j$$(nproc) test_core 2>&1 | tail -1
	@cp build/test_core build/test_core_cov

# --- Clean ---

clean:
	@rm -rf build/CMakeFiles build/CMakeCache.txt build/*.a build/*.so build/tranfi build/test_core build/bench
	@cd js && npx node-gyp clean 2>/dev/null || true
	@echo "Clean OK"
