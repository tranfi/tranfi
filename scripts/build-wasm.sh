#!/usr/bin/env bash
# Build WASM binary and copy to packages/js/wasm/
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

echo "Copying to packages/js/wasm/..."
mkdir -p "$ROOT/packages/js/wasm"
cp tranfi_core.js tranfi_core.wasm "$ROOT/packages/js/wasm/"

echo "Done. Files:"
ls -lh "$ROOT/packages/js/wasm/tranfi_core."*
