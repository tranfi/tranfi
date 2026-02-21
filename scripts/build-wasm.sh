#!/usr/bin/env bash
# Build WASM binary and copy to js/wasm/
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Load emscripten
if [ -f "$HOME/tools/emsdk/emsdk_env.sh" ]; then
  source "$HOME/tools/emsdk/emsdk_env.sh" 2>/dev/null
elif command -v emcc &>/dev/null; then
  : # emcc already available
else
  echo "Error: emscripten not found. Install emsdk to ~/tools/emsdk/" >&2
  exit 1
fi

echo "Building WASM..."
mkdir -p "$ROOT/build-wasm"
cd "$ROOT/build-wasm"
emcmake cmake .. -DCMAKE_BUILD_TYPE=Release
emmake make tranfi_wasm -j"$(nproc)"

echo "Copying to js/wasm/..."
mkdir -p "$ROOT/js/wasm"
cp tranfi_core.js "$ROOT/js/wasm/"
# SINGLE_FILE mode embeds WASM in JS — no separate .wasm file
if [ -f tranfi_core.wasm ]; then
  cp tranfi_core.wasm "$ROOT/js/wasm/"
fi

echo "Done."
ls -lh "$ROOT/js/wasm/tranfi_core."*
