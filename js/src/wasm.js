/**
 * wasm.js — Load the WASM module.
 * Lazy-initializes on first use.
 */

let instance = null

async function loadWasm() {
  if (instance) return instance

  // Load the WASM module
  const createTranfi = require('../wasm/tranfi_core.js')
  instance = await createTranfi()
  return instance
}

function getWasm() {
  return instance
}

module.exports = { loadWasm, getWasm }
