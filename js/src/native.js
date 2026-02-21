/**
 * native.js — Load the N-API native addon.
 * Returns the binding object or null if not available.
 */

import { createRequire } from 'module'

let binding = null

try {
  const require = createRequire(import.meta.url)
  binding = require('../build/Release/tranfi_napi.node')
} catch {
  try {
    const require = createRequire(import.meta.url)
    binding = require('../build/Debug/tranfi_napi.node')
  } catch {
    binding = null
  }
}

export default binding
