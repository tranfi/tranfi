/**
 * native.js — Load the N-API native addon.
 * Returns the binding object or null if not available.
 */

let binding = null

try {
  binding = require('../build/Release/tranfi_napi.node')
} catch {
  try {
    binding = require('../build/Debug/tranfi_napi.node')
  } catch {
    binding = null
  }
}

module.exports = binding
