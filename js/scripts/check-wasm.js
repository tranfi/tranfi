'use strict'

// Exercise the packaged engine, not just file presence, before accepting a
// toolchain-free install. No generated files or build tools are required.
require('../wasm')().then(backend => {
  backend.safeTransformLimits()
  if (typeof backend.TransformRecipe.fromJSON !== 'function' ||
      typeof backend.TransformPlan.fromBytes !== 'function') {
    throw new Error('packaged WASM does not expose prepared transforms')
  }
}).catch(error => {
  console.error(`tranfi: invalid packaged WASM runtime: ${error.message}`)
  process.exitCode = 1
})
