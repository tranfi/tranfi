'use strict'

const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const vm = require('node:vm')
const { createRequire } = require('node:module')
const test = require('node:test')

const file = path.resolve(__dirname, '../js/src/transform.js')
const source = fs.readFileSync(file, 'utf8')
const realRequire = createRequire(file)
const methods = [...source.matchAll(/callNative\(\s*'(\w+)'/g)].map(match => match[1])
function transform(binding) {
  const context = { module: { exports: {} }, require: id => id === './native.js' ? binding : realRequire(id) }
  vm.runInNewContext(source, context, { filename: file })
  return context.module.exports
}

test('prepared capability requires a complete callable native interface', () => {
  const complete = Object.fromEntries(methods.map(name => [name, () => ({})]))
  assert.equal(transform(null).hasNativePreparedTransforms(), false)
  assert.equal(transform({}).hasNativePreparedTransforms(), false)
  assert.equal(transform(complete).hasNativePreparedTransforms(), true)
  for (const method of methods) {
    assert.equal(transform({ ...complete, [method]: null }).hasNativePreparedTransforms(), false, method)
  }
})

test('capability detection does not consume or suppress native runtime errors', () => {
  const binding = Object.fromEntries(methods.map(name => [name, () => ({})]))
  binding.transformSafeLimits = () => { throw new Error('native runtime failure') }
  const api = transform(binding)
  assert.equal(api.hasNativePreparedTransforms(), true)
  assert.throws(() => api.safeTransformLimits(), /native runtime failure/)
  assert.throws(() => transform(null).safeTransformLimits(), /tranfi\/wasm/)
})
