SHELL := /bin/bash
.PHONY: all build test clean wasm
.PHONY: build-c build-node build-wasm
.PHONY: test-c test-python test-node test-parity
.PHONY: publish-python publish-node publish-github

all: build test

# --- Build targets ---

build: build-c build-node

build-c:
	@mkdir -p build
	@cd build && cmake .. -DBUILD_TESTING=ON -DCMAKE_BUILD_TYPE=Release > /dev/null 2>&1
	@cd build && make -j$$(nproc) 2>&1 | tail -1
	@echo "  C core OK"

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

test-python: build-c
	@source $$HOME/tools/miniconda3/etc/profile.d/conda.sh && conda activate base && \
		TRANFI_LIB_PATH=build/libtranfi.so python -m pytest test/test_python.py test/test_parity.py -v --tb=short

test-node: build-node
	@source $$HOME/.nvm/nvm.sh && node test/test_node.js

test-parity: build-c
	@source $$HOME/tools/miniconda3/etc/profile.d/conda.sh && conda activate base && \
		TRANFI_LIB_PATH=build/libtranfi.so python -m pytest test/test_parity.py -v --tb=short

# --- Publish targets ---

publish-python:
	@source $$HOME/tools/miniconda3/etc/profile.d/conda.sh && conda activate base && \
		cd py && rm -rf csrc dist && mkdir -p csrc && cp ../src/*.c ../src/*.h csrc/ && \
		python -m build --sdist && twine upload dist/*.tar.gz

publish-node: build-wasm
	@source $$HOME/.nvm/nvm.sh && cd js && npm publish

publish-github:
	@bash scripts/release.sh

# --- Clean ---

clean:
	@rm -rf build/CMakeFiles build/CMakeCache.txt build/*.a build/*.so build/tranfi build/test_core build/bench
	@source $$HOME/.nvm/nvm.sh && cd js && npx node-gyp clean 2>/dev/null || true
	@echo "Clean OK"
