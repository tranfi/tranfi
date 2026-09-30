'use strict'

const assert = require('node:assert/strict')
const test = require('node:test')
const { installNative } = require('../js/scripts/install-native.js')

function exercise({ env = {}, platform = 'linux', build = { status: 0, signal: null }, sync = { status: 0, signal: null }, wasm = { status: 0, signal: null } } = {}) {
  const calls = []
  const messages = []
  const status = installNative({
    env, platform, execPath: '/tool/node',
    logger: { error: message => messages.push(message), warn: message => messages.push(message) },
    spawn(command, args, options) {
      calls.push({ command, args, options })
      if (args[0].endsWith('sync-csrc.js')) return sync
      if (args[0].endsWith('check-wasm.js')) return wasm
      return build
    }
  })
  return { status, calls, messages }
}

test('native installer preserves source synchronization failures', () => {
  const result = exercise({ sync: { status: 7, signal: null } })
  assert.equal(result.status, 7)
  assert.match(result.messages.join('\n'), /synchronization.*status 7/)
})

test('missing toolchain or native compilation failure uses verified WASM', () => {
  for (const build of [
    { status: 9, signal: null },
    { status: null, signal: null, error: new Error('ENOENT') },
    { status: null, signal: 'SIGTERM' }
  ]) {
    const result = exercise({ build })
    assert.equal(result.status, 0)
    assert.match(result.messages.join('\n'), /WASM/)
    assert(result.calls.some(call => call.args[0].endsWith('check-wasm.js')))
  }
})

test('npm node-gyp script runs under the current Node executable', () => {
  const result = exercise({ env: { npm_config_node_gyp: '/tool/node-gyp.js' } })
  assert.equal(result.status, 0)
  const call = result.calls.find(call => call.args[0] === '/tool/node-gyp.js')
  assert.equal(call.command, '/tool/node')
  assert.deepEqual(call.args, ['/tool/node-gyp.js', 'rebuild'])
  assert.equal(call.options.shell, false)
})

test('Windows uses packaged WASM without attempting a native build', () => {
  const result = exercise({ platform: 'win32' })
  assert.equal(result.status, 0)
  assert.equal(result.calls.length, 1)
  assert(result.calls[0].args[0].endsWith('check-wasm.js'))
})

test('explicit skip still verifies the packaged WASM runtime', () => {
  const result = exercise({ env: { TRANFI_SKIP_NATIVE_BUILD: '1' } })
  assert.equal(result.status, 0)
  assert.equal(result.calls.length, 1)
  assert(result.calls[0].args[0].endsWith('check-wasm.js'))
})

test('missing or invalid packaged WASM remains fatal, including explicit skip', () => {
  for (const env of [{}, { TRANFI_SKIP_NATIVE_BUILD: '1' }]) {
    const result = exercise({ env, wasm: { status: 3, signal: null } })
    assert.equal(result.status, 3)
    assert.match(result.messages.join('\n'), /WASM.*status 3/)
    assert(!result.calls.some(call => call.args.includes('rebuild')))
  }
})
