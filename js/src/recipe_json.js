'use strict'

const { TranfiTransformError } = require('./transform_error.js')

// The replacer runs before JSON.stringify emits each value. Charge the exact
// UTF-8 wire size there, so neither a giant string nor many small fields can
// allocate an over-budget JSON result. Getters/toJSON are evaluated only once.
function stringifyRecipe(recipe, limits) {
  const limit = Math.min(limits.maxRecipeBytes, limits.maxAllocationBytes - 1)
  let bytes = 0
  const parents = []
  const counts = []
  function charge(n) {
    bytes += n
    if (bytes > limit) {
      throw new TranfiTransformError(104, 'recipe exceeds its configured byte limit')
    }
  }
  function string(value) {
    charge(2)
    for (let i = 0; i < value.length; i++) {
      const c = value.charCodeAt(i)
      if (c >= 0xd800 && c <= 0xdbff) {
        const next = value.charCodeAt(++i)
        if (!(next >= 0xdc00 && next <= 0xdfff)) {
          throw new TypeError('recipe must not contain an unpaired UTF-16 surrogate')
        }
        charge(4)
      } else if (c >= 0xdc00 && c <= 0xdfff) {
        throw new TypeError('recipe must not contain an unpaired UTF-16 surrogate')
      } else if (c === 34 || c === 92 || (c >= 8 && c <= 10) || c === 12 || c === 13) charge(2)
      else if (c < 32) charge(6)
      else charge(c < 128 ? 1 : c < 2048 ? 2 : 3)
    }
  }
  return JSON.stringify(recipe, function(key, value) {
    while (parents.length && parents[parents.length - 1] !== this) {
      parents.pop()
      counts.pop()
    }
    // JSON's boxed primitives are scalars. Use its conversion semantics before
    // counting; returning the scalar prevents a second custom coercion.
    if (value && typeof value === 'object') {
      let boxed = false
      try { Number.prototype.valueOf.call(value); boxed = true } catch (_) {}
      if (boxed) value = Number(value)
      else {
        try { String.prototype.valueOf.call(value); boxed = true } catch (_) {}
        if (boxed) value = String(value)
        else {
          try { value = Boolean.prototype.valueOf.call(value) } catch (_) {}
        }
      }
    }
    const type = typeof value
    const omitted = value === undefined || type === 'function' || type === 'symbol'
    if (omitted && !Array.isArray(this)) return undefined
    if (parents.length) {
      const last = parents.length - 1
      if (counts[last]++) charge(1)
      if (!Array.isArray(this)) { string(key); charge(1) }
    }
    if (value === null || omitted) charge(4)
    else if (type === 'string') string(value)
    else if (type === 'number') charge(Number.isFinite(value) ? String(value).length : 4)
    else if (type === 'boolean') charge(value ? 4 : 5)
    else if (type === 'object') {
      if (parents.includes(value)) throw new TypeError('circular recipe')
      if (parents.length + 1 > limits.maxJsonDepth) {
        throw new TranfiTransformError(104, 'recipe exceeds its configured depth limit')
      }
      charge(2)
      parents.push(value)
      counts.push(0)
    }
    return value
  })
}

module.exports = { stringifyRecipe }
