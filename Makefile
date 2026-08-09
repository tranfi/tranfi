SHELL := /bin/bash
.SHELLFLAGS := -o pipefail -c
PYTHON ?= python
NPM ?= npm
NODE ?= node
TWINE ?= twine
PYTEST ?= $(PYTHON) -m pytest
PACKAGE_TMPDIR ?= $(CURDIR)/build/tmp
PACKAGE_ENV := TMPDIR="$(PACKAGE_TMPDIR)" TEMP="$(PACKAGE_TMPDIR)" TMP="$(PACKAGE_TMPDIR)" TRANFI_TEST_TMPDIR="$(PACKAGE_TMPDIR)/package-smoke" PIP_CACHE_DIR="$(PACKAGE_TMPDIR)/pip-cache" npm_config_cache="$(PACKAGE_TMPDIR)/npm-cache"
PY_BUILD_ENV := $(PACKAGE_ENV)
BUILD_TMPDIR ?= $(PACKAGE_TMPDIR)
BUILD_ENV := TMPDIR="$(BUILD_TMPDIR)" TEMP="$(BUILD_TMPDIR)" TMP="$(BUILD_TMPDIR)"
TEST_TMPDIR ?= $(BUILD_TMPDIR)/test
TEST_MEMORY_ENV := $(BUILD_ENV) TRANFI_TEST_TMPDIR="$(TEST_TMPDIR)/memory"
TEST_OOM_ENV := $(BUILD_ENV) TRANFI_TEST_TMPDIR="$(TEST_TMPDIR)/oom"
TEST_PYTHON_ENV := $(BUILD_ENV) TRANFI_TEST_TMPDIR="$(TEST_TMPDIR)/python"
TEST_NODE_ENV := $(BUILD_ENV) TRANFI_TEST_TMPDIR="$(TEST_TMPDIR)/node"
NODE_GYP_NODEDIR ?= $(shell test -f /usr/local/include/node/common.gypi && printf /usr/local)
NODE_BUILD_ENV := $(BUILD_ENV) npm_config_cache="$(BUILD_TMPDIR)/npm-cache"
ifneq ($(NODE_GYP_NODEDIR),)
NODE_BUILD_ENV += npm_config_nodedir="$(NODE_GYP_NODEDIR)"
endif
SANITIZER_RUN := $(shell if command -v setarch >/dev/null 2>&1 && setarch "$$(uname -m)" -R true >/dev/null 2>&1; then printf 'setarch %s -R' "$$(uname -m)"; fi)
ASAN_OPTIONS ?= halt_on_error=1
ASAN_RUN := $(SANITIZER_RUN) env ASAN_OPTIONS="$(ASAN_OPTIONS)"
BENCH_ROWS ?= 1000000
BENCH_SMOKE_ROWS ?= 10000

.PHONY: all build test clean wasm app site verify bench bench-smoke fuzz fuzz-smoke fuzz-nightly fuzz-c-csv fuzz-c-expr fuzz-c-selector fuzz-c-dsl fuzz-c-jsonl fuzz-c-jsonpath
.PHONY: build-c build-debug build-tsan build-node build-wasm build-js build-py sync-js-csrc sync-py-csrc
.PHONY: check-js-csrc-sync check-py-csrc-sync check-csrc-sync sbom test-packaging test-packaging-install
.PHONY: test-packaging-node test-packaging-node-install test-packaging-python test-packaging-python-install test-properties
.PHONY: test-c test-memory test-debug test-tsan test-oom test-python test-node test-parity test-duckdb test-vectors
.PHONY: test-spill-sec test-depth-limits test-float-rt test-wide-csv
.PHONY: publish-python publish-node publish-github

all: build test

# --- Build targets ---

build: build-c build-node

build-c:
	@mkdir -p build "$(BUILD_TMPDIR)"
	@cd build && env $(BUILD_ENV) cmake .. -DBUILD_TESTING=ON -DCMAKE_BUILD_TYPE=Release > /dev/null 2>&1
	@cd build && env $(BUILD_ENV) make -j$$(nproc) 2>&1 | tail -1
	@echo "  C core OK"

build-debug:
	@mkdir -p build-debug "$(BUILD_TMPDIR)"
	@cd build-debug && env $(BUILD_ENV) cmake .. -DBUILD_TESTING=ON -DCMAKE_BUILD_TYPE=Debug > /dev/null 2>&1
	@cd build-debug && env $(BUILD_ENV) make -j$$(nproc) 2>&1 | tail -1
	@echo "  C core (Debug+ASan/UBSan) OK"

build-tsan:
	@mkdir -p build-tsan "$(BUILD_TMPDIR)"
	@cd build-tsan && env $(BUILD_ENV) cmake .. -DBUILD_TESTING=ON -DCMAKE_BUILD_TYPE=Debug -DTRANFI_SANITIZER=thread > /dev/null 2>&1
	@cd build-tsan && env $(BUILD_ENV) make -j$$(nproc) test_core 2>&1 | tail -1
	@echo "  C core (TSan) OK"

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

sbom:
	@$(PYTHON) scripts/generate-sbom.py --out build/tranfi-sbom.spdx.json

build-node: build-c sync-js-csrc check-js-csrc-sync
	@mkdir -p "$(BUILD_TMPDIR)/npm-cache"
	@cd js && env $(NODE_BUILD_ENV) $(NPM) run build:native 2>&1 | tail -1
	@echo "  Node.js N-API OK"

build-js: build-node build-wasm

build-py: sync-py-csrc check-py-csrc-sync
	@mkdir -p "$(PACKAGE_TMPDIR)"
	@cd py && rm -rf dist build *.egg-info && $(PY_BUILD_ENV) $(PYTHON) -m build --sdist
	@$(PYTHON) scripts/audit-package-artifacts.py --python-sdist "py/dist/tranfi-*.tar.gz"

build-wasm:
	@bash scripts/build-wasm.sh 2>&1 | tail -3

wasm: build-wasm

# --- Test targets ---

test: test-vectors test-c test-python test-properties test-node test-packaging fuzz-smoke

test-vectors:
	@$(PYTHON) test/validate_transform_vectors.py

test-c: build-c
	@bash scripts/check-local-diagnostics.sh
	@cmake --build build --target test_memory test_core test_wasm_api test_transform > /dev/null
	@mkdir -p "$(TEST_TMPDIR)/memory"
	@env $(TEST_MEMORY_ENV) ./build/test_memory
	@env $(BUILD_ENV) ./build/test_core
	@env $(BUILD_ENV) ./build/test_wasm_api
	@env $(BUILD_ENV) ./build/test_transform
	@env $(BUILD_ENV) bash test/test_cli_memory_policy.sh ./build/tranfi

test-memory: build-c
	@cmake --build build --target test_memory > /dev/null
	@mkdir -p "$(TEST_TMPDIR)/memory"
	@env $(TEST_MEMORY_ENV) ./build/test_memory

test-debug: build-debug
	@mkdir -p "$(TEST_TMPDIR)/memory"
	@$(ASAN_RUN) env $(BUILD_ENV) ./build-debug/test_core
	@$(ASAN_RUN) env $(TEST_MEMORY_ENV) ./build-debug/test_memory
	@$(ASAN_RUN) env $(BUILD_ENV) ./build-debug/test_wasm_api
	@$(ASAN_RUN) env $(BUILD_ENV) ./build-debug/test_transform
	@$(ASAN_RUN) env $(BUILD_ENV) bash test/test_cli_memory_policy.sh ./build-debug/tranfi

test-tsan: build-tsan
	@$(SANITIZER_RUN) env $(BUILD_ENV) TSAN_OPTIONS=halt_on_error=1 ./build-tsan/test_core test_thread_local_last_error

test-oom: build-debug
	@mkdir -p "$(TEST_TMPDIR)/oom"
	@$(ASAN_RUN) env $(TEST_OOM_ENV) ./build-debug/test_oom

test-spill-sec: build-debug
	@$(ASAN_RUN) env $(BUILD_ENV) ./build-debug/test_core \
		test_spill_session_security_basics \
		test_spill_session_cleanup_after_abort \
		test_spill_sort_uses_private_session_dir

test-depth-limits: build-debug
	@$(ASAN_RUN) env $(BUILD_ENV) ./build-debug/test_core \
		test_expr_depth_limit \
		test_selector_depth_limit \
		test_json_path_depth_limit

test-float-rt: build-debug
	@$(ASAN_RUN) env $(BUILD_ENV) ./build-debug/test_core \
		test_pipeline_float_roundtrip_bits

test-wide-csv: build-debug
	@$(ASAN_RUN) env $(BUILD_ENV) ./build-debug/test_core \
		test_pipeline_csv_wide_columns \
		test_pipeline_csv_max_columns

test-python: build-c
	@mkdir -p "$(TEST_TMPDIR)/python"
	@env $(TEST_PYTHON_ENV) PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 TRANFI_LIB_PATH=build/libtranfi.so \
		$(PYTEST) test/test_python.py test/test_transform_python.py test/test_parity.py test/test_duckdb.py -v --tb=short

test-node: build-node build-wasm
	@mkdir -p "$(TEST_TMPDIR)/node"
	@env $(TEST_NODE_ENV) $(NODE) test/test_node.js
	@env $(TEST_NODE_ENV) $(NODE) test/test_transform_node.js
	@env $(TEST_NODE_ENV) $(NODE) test/test_transform_wasm_node.js
	@env $(TEST_NODE_ENV) $(NODE) test/test_transform_worker_node.js

test-parity: build-c
	@mkdir -p "$(TEST_TMPDIR)/python"
	@env $(TEST_PYTHON_ENV) PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 TRANFI_LIB_PATH=build/libtranfi.so \
		$(PYTEST) test/test_parity.py -v --tb=short

test-duckdb: build-c
	@mkdir -p "$(TEST_TMPDIR)/python"
	@env $(TEST_PYTHON_ENV) PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 TRANFI_LIB_PATH=build/libtranfi.so \
		$(PYTEST) test/test_duckdb.py -v --tb=short

test-properties: build-c
	@mkdir -p "$(TEST_TMPDIR)/python"
	@env $(TEST_PYTHON_ENV) PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 TRANFI_LIB_PATH=build/libtranfi.so \
		$(PYTEST) test/test_properties.py -v --tb=short

test-packaging: sync-py-csrc sync-js-csrc check-csrc-sync sbom test-packaging-python test-packaging-node test-packaging-install

test-packaging-python: sbom sync-py-csrc check-py-csrc-sync
	@mkdir -p "$(PACKAGE_TMPDIR)"
	@cd py && rm -rf dist build *.egg-info && $(PY_BUILD_ENV) $(PYTHON) -m build --sdist
	@$(PYTHON) scripts/audit-package-artifacts.py --python-sdist "py/dist/tranfi-*.tar.gz"

test-packaging-node: sbom sync-js-csrc check-js-csrc-sync
	@mkdir -p "$(PACKAGE_TMPDIR)" build
	@cd js && $(PACKAGE_ENV) $(NPM) pack --dry-run --json > ../build/npm-pack-dry-run.json
	@$(PYTHON) scripts/audit-package-artifacts.py --npm-json build/npm-pack-dry-run.json

test-packaging-python-install: test-packaging-python
	@$(PACKAGE_ENV) $(PYTHON) scripts/smoke-install-packages.py --python-sdist "py/dist/tranfi-*.tar.gz" --python "$(PYTHON)"

test-packaging-node-install: test-packaging-node
	@rm -rf build/npm-install-smoke && mkdir -p "$(PACKAGE_TMPDIR)" build/npm-install-smoke
	@cd js && $(PACKAGE_ENV) $(NPM) pack --json --pack-destination ../build/npm-install-smoke > ../build/npm-pack-install.json
	@$(PYTHON) scripts/audit-package-artifacts.py --npm-json build/npm-pack-install.json
	@$(PACKAGE_ENV) $(PYTHON) scripts/smoke-install-packages.py --npm-tarball "build/npm-install-smoke/tranfi-*.tgz" --node "$(NODE)" --npm "$(NPM)"

test-packaging-install: test-packaging-python-install test-packaging-node-install

# --- Benchmarks ---

bench: build-c
	@./build/bench $(BENCH_ROWS)

bench-smoke: build-c
	@./build/bench $(BENCH_SMOKE_ROWS) >/dev/null
	@echo "  Bench smoke OK"

# --- Fuzz testing ---

fuzz: fuzz-c-csv fuzz-c-expr fuzz-c-selector fuzz-c-dsl fuzz-c-jsonl fuzz-c-jsonpath

fuzz-smoke:
	$(MAKE) fuzz FUZZ_ARGS="$(FUZZ_SMOKE_ARGS)"

fuzz-nightly:
	$(MAKE) fuzz FUZZ_ARGS="$(FUZZ_NIGHTLY_ARGS)"

fuzz-c-csv: build/fuzz_csv
	@mkdir -p $(FUZZ_WORK_DIR)/csv
	$(ASAN_RUN) ./build/fuzz_csv $(FUZZ_WORK_DIR)/csv $(FUZZ_SEED_DIR)/csv $(FUZZ_ARGS)

fuzz-c-expr: build/fuzz_expr
	@mkdir -p $(FUZZ_WORK_DIR)/expr
	$(ASAN_RUN) ./build/fuzz_expr $(FUZZ_WORK_DIR)/expr $(FUZZ_SEED_DIR)/expr $(FUZZ_ARGS)

fuzz-c-selector: build/fuzz_selector
	@mkdir -p $(FUZZ_WORK_DIR)/selector
	$(ASAN_RUN) ./build/fuzz_selector $(FUZZ_WORK_DIR)/selector $(FUZZ_SEED_DIR)/selector $(FUZZ_ARGS)

fuzz-c-dsl: build/fuzz_dsl
	@mkdir -p $(FUZZ_WORK_DIR)/dsl
	$(ASAN_RUN) ./build/fuzz_dsl $(FUZZ_WORK_DIR)/dsl $(FUZZ_SEED_DIR)/dsl $(FUZZ_ARGS)

fuzz-c-jsonl: build/fuzz_jsonl
	@mkdir -p $(FUZZ_WORK_DIR)/jsonl
	$(ASAN_RUN) ./build/fuzz_jsonl $(FUZZ_WORK_DIR)/jsonl $(FUZZ_SEED_DIR)/jsonl $(FUZZ_ARGS)

fuzz-c-jsonpath: build/fuzz_jsonpath
	@mkdir -p $(FUZZ_WORK_DIR)/jsonpath
	$(ASAN_RUN) ./build/fuzz_jsonpath $(FUZZ_WORK_DIR)/jsonpath $(FUZZ_SEED_DIR)/jsonpath $(FUZZ_ARGS)

FUZZ_CC ?= clang
FUZZ_ARGS ?= -runs=256 -max_len=4096 -timeout=5
FUZZ_SMOKE_ARGS ?= -runs=256 -max_len=4096 -timeout=5
FUZZ_NIGHTLY_ARGS ?= -runs=8192 -max_len=8192 -timeout=10
FUZZ_WORK_DIR ?= corpus
FUZZ_SEED_DIR ?= test/corpus
FUZZ_TMPDIR ?= $(CURDIR)/build/tmp
FUZZ_SRC = $(filter-out src/main.c,$(wildcard src/*.c))
FUZZ_HEADERS = $(wildcard src/*.h)
FUZZ_CFLAGS = -std=c11 -g -O1 -fsanitize=fuzzer,address,undefined \
	-D_POSIX_C_SOURCE=200809L -D_XOPEN_SOURCE=700 -I src \
	-Werror=implicit-function-declaration -Werror=incompatible-pointer-types \
	-Wformat -Werror=format-security \
	-Werror=unused-result \
	-fno-common -fstack-protector-strong

build/fuzz_%: test/fuzz_%.c $(FUZZ_SRC) $(FUZZ_HEADERS)
	@mkdir -p build "$(FUZZ_TMPDIR)"
	TMPDIR="$(FUZZ_TMPDIR)" $(FUZZ_CC) $(FUZZ_CFLAGS) $< $(FUZZ_SRC) -lm -o $@

# --- Verify (full suite with sanitizers) ---

verify: test-vectors build-debug test-debug test-spill-sec test-depth-limits test-float-rt test-wide-csv test-oom test-tsan test-python test-properties test-node test-packaging fuzz-smoke

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
	@mkdir -p "$(PACKAGE_TMPDIR)"
	@cd py && rm -rf dist && \
		$(PYTHON) scripts/sync-csrc.py && \
		$(PYTHON) ../scripts/check-csrc-sync.py --mirror py && \
		rm -rf tranfi/app && cp -r ../app/dist tranfi/app && rm -rf tranfi/app/wasm tranfi/app/lib && \
		$(PY_BUILD_ENV) $(PYTHON) -m build --sdist && \
		$(PACKAGE_ENV) $(PYTHON) ../scripts/audit-package-artifacts.py --python-sdist "dist/tranfi-*.tar.gz" && \
		$(PACKAGE_ENV) $(PYTHON) ../scripts/smoke-install-packages.py --python-sdist "dist/tranfi-*.tar.gz" --python "$(PYTHON)" && \
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
