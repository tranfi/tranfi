/**
 * wasm.js — Load the WASM module.
 * Lazy-initializes on first use.
 */

let instance = null

export async function loadWasm() {
  if (instance) return instance

  // Try to load the WASM module
  const { default: createTranfi } = await import('../wasm/tranfi_core.js')
  instance = await createTranfi()
  return instance
}

export function getWasm() {
  return instance
}
