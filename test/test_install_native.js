'use strict'

const assert = require('node:assert/strict')
const test = require('node:test')
const { installNative } = require('../js/scripts/install-native.js')

function logger() {
  const messages = { error: [], warn: [] }
  return {
    messages,
    error(message) { messages.error.push(message) },
    warn(message) { messages.warn.push(message) }
  }
}

test('native installer propagates synchronization failure', () => {
  const log = logger()
  let calls = 0
  const status = installNative({
    env: {},
    logger: log,
    spawn() {
      calls++
      return { status: 7, signal: null }
    }
  })
  assert.equal(status, 7)
  assert.equal(calls, 1)
  assert.match(log.messages.error[0], /synchronization.*status 7/)
})

test('native installer propagates node-gyp failure', () => {
  const log = logger()
  const commands = []
  const invocations = []
  const status = installNative({
    env: { npm_config_node_gyp: '/tool/node-gyp.js' },
    logger: log,
    execPath: '/tool/node',
    spawn(command, args, options) {
      commands.push(command)
      invocations.push({ args, options })
      return commands.length === 1
        ? { status: 0, signal: null }
        : { status: 9, signal: null }
    }
  })
  assert.equal(status, 9)
  assert.equal(commands[1], '/tool/node')
  assert.deepEqual(invocations[1].args, ['/tool/node-gyp.js', 'rebuild'])
  assert.equal(invocations[1].options.shell, false)
  assert.match(log.messages.error.join('\n'), /native addon build.*status 9/)
  assert.match(log.messages.error.join('\n'), /TRANFI_SKIP_NATIVE_BUILD=1/)
})

test('Windows command fallback uses the shell for the node-gyp shim', () => {
  const invocations = []
  const status = installNative({
    env: {},
    platform: 'win32',
    spawn(command, args, options) {
      invocations.push({ command, args, options })
      return { status: 0, signal: null }
    },
  })
  assert.equal(status, 0)
  assert.equal(invocations[1].command, 'node-gyp')
  assert.deepEqual(invocations[1].args, ['rebuild'])
  assert.equal(invocations[1].options.shell, true)
})

test('explicit WASM-only install skip is successful and visible', () => {
  const log = logger()
  const status = installNative({
    env: { TRANFI_SKIP_NATIVE_BUILD: '1' },
    logger: log,
    spawn() { throw new Error('must not spawn') }
  })
  assert.equal(status, 0)
  assert.match(log.messages.warn[0], /tranfi\/wasm/)
})
