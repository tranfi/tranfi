SHELL := /bin/bash
.PHONY: all build test clean wasm app site verify fuzz
.PHONY: build-c build-debug build-node build-wasm test-properties
.PHONY: test-c test-debug test-python test-node test-parity
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

build-node: build-c
	@source $$HOME/.nvm/nvm.sh && cd js && npm run build:native 2>&1 | tail -1
	@echo "  Node.js N-API OK"

build-wasm:
	@bash scripts/build-wasm.sh 2>&1 | tail -3

wasm: build-wasm

# --- Test targets ---

test: test-c test-python test-node

test-c: build-c
	@./build/test_core

test-debug: build-debug
	@ASAN_OPTIONS=detect_leaks=0 ./build-debug/test_core

test-python: build-c
	@source $$HOME/tools/miniconda3/etc/profile.d/conda.sh && conda activate base && \
		TRANFI_LIB_PATH=build/libtranfi.so python -m pytest test/test_python.py test/test_parity.py -v --tb=short

test-node: build-node
	@source $$HOME/.nvm/nvm.sh && node test/test_node.js

test-parity: build-c
	@source $$HOME/tools/miniconda3/etc/profile.d/conda.sh && conda activate base && \
		TRANFI_LIB_PATH=build/libtranfi.so python -m pytest test/test_parity.py -v --tb=short

test-properties: build-c
	@source $$HOME/tools/miniconda3/etc/profile.d/conda.sh && conda activate base && \
		TRANFI_LIB_PATH=build/libtranfi.so python -m pytest test/test_properties.py -v --tb=short

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

verify: build-debug test-debug test-python test-node

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
	@source $$HOME/tools/miniconda3/etc/profile.d/conda.sh && conda activate base && \
		cd py && rm -rf csrc dist && mkdir -p csrc && cp ../src/*.c ../src/*.h csrc/ && \
		rm -rf tranfi/app && cp -r ../app/dist tranfi/app && rm -rf tranfi/app/wasm tranfi/app/lib && \
		python -m build --sdist && twine upload dist/*.tar.gz

publish-node: build-wasm
	@source $$HOME/.nvm/nvm.sh && cd js && npm publish

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
	@source $$HOME/.nvm/nvm.sh && cd js && npx node-gyp clean 2>/dev/null || true
	@echo "Clean OK"
